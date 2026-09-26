"""D8 replicate calibration seeds and the registered-look record (prereg 3.6.0).

The replicate seeds (corpora.calibration_replicate_seeds) must not alias any
registered or spent seed through the generator's seed arithmetic, and must
pass the AML seed guard. A registered evaluation or robustness look records
its seed when it starts and its report sha256 before any verdict is printed;
every recorded seed is spent.
"""

from __future__ import annotations

import importlib.util
import itertools
import json
from pathlib import Path

import numpy as np
import pytest

from lakebench.config import datagen_seed as ds

ROOT = Path(__file__).resolve().parents[1]
PREREG = json.loads((ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json").read_text())
CORPORA = PREREG["corpora"]
REPLICATES = CORPORA["calibration_replicate_seeds"]
FIXED = sorted(
    {
        CORPORA["calibration_seed"],
        CORPORA["evaluation_seed"],
        CORPORA["robustness_seed"],
        *CORPORA["spent_seeds"],
    }
)
TID_SEED_STRIDE = 100_000_000  # datagen_rs::typology::TID_SEED_STRIDE
N_TIDS = 15  # datagen_rs typology tids 0..=14
SALT_SPAN = 2048  # datagen salts are seed + offsets below this (world.rs, kyc.rs, realism.rs)
POPULATION_BITS = 24  # 2^24 > round(111111 x 100) entities at scale 100


def _gap_ok(a: int, b: int) -> bool:
    r = abs(a - b) % TID_SEED_STRIDE
    return min(r, TID_SEED_STRIDE - r) >= PREREG["seed_guard"]["max_n_inst_scale100"]


def test_replicates_are_four_distinct_new_seeds():
    assert len(REPLICATES) == len(set(REPLICATES)) == 4
    assert not set(REPLICATES) & set(FIXED)
    assert 44 not in REPLICATES and 45 not in REPLICATES


@pytest.mark.parametrize("seed", REPLICATES)
def test_replicate_passes_the_seed_guard(seed):
    assert ds.aml_seed_error(CORPORA, seed) is None
    for other in [*FIXED, *REPLICATES]:
        if other != seed:
            assert _gap_ok(seed, other), (seed, other)
            assert abs(seed - other) > 2 * SALT_SPAN, (seed, other)


def test_replicates_cannot_alias_through_xor_salted_hashes():
    offs = np.arange(SALT_SPAN, dtype=np.int64)
    for a in REPLICATES:
        for b in [*FIXED, *REPLICATES]:
            if a == b:
                continue
            x = (np.int64(a) + offs)[:, None] ^ (np.int64(b) + offs)[None, :]
            assert int(np.abs(x).min()) >= 1 << POPULATION_BITS, (a, b)


def _splitmix64(x):
    x = np.asarray(x, dtype=np.uint64)
    with np.errstate(over="ignore"):
        z = x + np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def test_replicate_instance_seeds_are_disjoint_at_scale_100():
    """D10 for the new seeds: typology.rs iseed = splitmix64(seed ^
    splitmix64(0xF100 + tid x TID_SEED_STRIDE + j)), n_inst up to
    seed_guard.max_n_inst_scale100 per tid."""
    n = PREREG["seed_guard"]["max_n_inst_scale100"]
    j = np.arange(n, dtype=np.uint64)
    inner = _splitmix64(
        np.concatenate(
            [
                np.uint64(0xF100) + np.uint64(t) * np.uint64(TID_SEED_STRIDE) + j
                for t in range(N_TIDS)
            ]
        )
    )
    sets = {s: np.unique(_splitmix64(np.uint64(s) ^ inner)) for s in [*FIXED, *REPLICATES]}
    for a, b in itertools.combinations(sets, 2):
        if a in REPLICATES or b in REPLICATES:
            assert len(np.intersect1d(sets[a], sets[b], assume_unique=True)) == 0, (a, b)


def test_replicates_are_calibration_to_the_gate():
    from lakebench.aml.fidelity_gate import corpus_role

    for s in REPLICATES:
        assert corpus_role(s, PREREG) == "calibration"
    assert corpus_role(7777, PREREG) == "other"


# ---------------------------------------------------------------------------
# Registered-look record
# ---------------------------------------------------------------------------


@pytest.fixture
def record(tmp_path):
    p = tmp_path / ds.LOOKS_FILENAME
    p.write_text(json.dumps({"_doc": "test", "looks": []}))
    return p


def test_tracked_record_exists_and_is_empty_before_any_look():
    assert ds.looks_path().name == ds.LOOKS_FILENAME
    assert ds.load_looks() == []


def test_claim_then_complete_spends_the_seed(record):
    ev = CORPORA["evaluation_seed"]
    ds.claim_look("evaluation", ev, {"out": "/x/gate.json"}, path=record)
    assert ev in ds.recorded_seeds(record)
    merged = ds.with_recorded_looks(CORPORA, record)
    assert "spent" in ds.aml_seed_error(merged, ev, "evaluation", [ev], claim_verified=True)
    with pytest.raises(ValueError, match="already has a recorded look"):
        ds.claim_look("evaluation", ev, path=record)
    e = ds.complete_look("evaluation", ev, "a" * 64, "/x/gate.json", path=record)
    assert e["state"] == "complete" and e["report_sha256"] == "a" * 64
    with pytest.raises(ValueError, match="already complete"):
        ds.complete_look("evaluation", ev, "b" * 64, "/x/gate.json", path=record)
    assert ds.load_looks(record)[0]["report_sha256"] == "a" * 64


def test_complete_without_a_started_look_is_refused(record):
    rb = CORPORA["robustness_seed"]
    with pytest.raises(ValueError, match="no started look"):
        ds.complete_look("robustness", rb, "c" * 64, "/x/r.json", path=record)
    assert ds.recorded_seeds(record) == frozenset()


def test_role_mismatch_and_calibration_are_refused(record):
    ev = CORPORA["evaluation_seed"]
    ds.claim_look("evaluation", ev, path=record)
    with pytest.raises(ValueError, match="claimed as"):
        ds.complete_look("robustness", ev, "d" * 64, "/x", path=record)
    with pytest.raises(ValueError, match="only"):
        ds.claim_look("calibration", CORPORA["calibration_seed"], path=record)


def test_malformed_or_missing_record_fails_closed(record, monkeypatch, tmp_path):
    record.write_text(json.dumps({"looks": [{"seed": "50000043", "role": "evaluation"}]}))
    with pytest.raises(ValueError, match="malformed"):
        ds.load_looks(record)
    record.write_text(json.dumps({"looks": {}}))
    with pytest.raises(ValueError):
        ds.recorded_seeds(record)
    monkeypatch.setattr(ds, "_PREREG_PATH", tmp_path / "nowhere" / "prereg.json")
    with pytest.raises(FileNotFoundError):
        ds.looks_path()


def _runner():
    spec = importlib.util.spec_from_file_location("aml_gate_runner", ROOT / "scripts/aml_gate.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_registered_look_records_the_hash_before_printing_the_verdict(
    record, tmp_path, monkeypatch
):
    mod = _runner()
    ev = CORPORA["evaluation_seed"]
    ds.claim_look("evaluation", ev, path=record)
    real_complete = ds.complete_look
    monkeypatch.setattr(
        ds, "complete_look", lambda *a, **k: real_complete(*a, **{**k, "path": record})
    )
    monkeypatch.setattr(ds, "load_predictions", lambda *a, **k: ({}, "p" * 64))
    out = tmp_path / "gate.json"
    printed = []

    def summary(report):
        # The verdict lines are generated only after the record holds the hash.
        entry = ds.load_looks(record)[0]
        assert entry["state"] == "complete"
        import hashlib

        assert entry["report_sha256"] == hashlib.sha256(out.read_bytes()).hexdigest()
        return ["VERDICT"]

    ok = mod.emit_report(
        {"verdict": "ok"},
        out,
        "evaluation",
        ev,
        summary,
        print_fn=printed.append,
        predictions_sha="p" * 64,
    )
    assert ok is True and "VERDICT" in printed


def test_registered_look_withholds_the_verdict_when_the_record_fails(tmp_path, monkeypatch):
    mod = _runner()

    def boom(*a, **k):
        raise OSError("read-only record")

    monkeypatch.setattr(ds, "complete_look", boom)
    monkeypatch.setattr(ds, "load_predictions", lambda *a, **k: ({}, "p" * 64))
    printed = []

    def summary(report):
        raise AssertionError("the verdict must not be printed")

    ok = mod.emit_report(
        {"verdict": "ok"},
        tmp_path / "gate.json",
        "evaluation",
        CORPORA["evaluation_seed"],
        summary,
        print_fn=lambda *a, **k: printed.append(a[0]),
        predictions_sha="p" * 64,
    )
    assert ok is False and any("verdict withheld" in p for p in printed)


def test_registered_look_withholds_the_verdict_when_predictions_change(
    record, tmp_path, monkeypatch
):
    mod = _runner()
    ev = CORPORA["evaluation_seed"]
    ds.claim_look("evaluation", ev, path=record)
    monkeypatch.setattr(ds, "load_predictions", lambda *a, **k: ({}, "q" * 64))
    printed = []

    def summary(report):
        raise AssertionError("the verdict must not be printed")

    ok = mod.emit_report(
        {"verdict": "ok"},
        tmp_path / "gate.json",
        "evaluation",
        ev,
        summary,
        print_fn=lambda *a, **k: printed.append(a[0]),
        predictions_sha="p" * 64,
    )
    assert ok is False and any("predictions changed" in p for p in printed)
    assert ds.load_looks(record)[0]["state"] == "started"


# ---------------------------------------------------------------------------
# Level-2 predictions record (#46 D-8)
# ---------------------------------------------------------------------------


def test_tracked_predictions_record_is_uncommitted_so_looks_refuse():
    with pytest.raises(ValueError, match="no committed predictions"):
        ds.load_predictions()
    mod = _runner()
    assert "not committed" in mod.predictions_error("x@sha256:" + "a" * 64)


def _pred_doc(**over):
    pred = {
        "prereg_sha256": "s" * 64,
        "generator_image": "img@sha256:" + "a" * 64,
        "pi_level": 0.95,
        "typologies": {"gather_scatter": {"predicted_ap": 0.7, "pi": [0.6, 0.8]}},
    }
    pred.update(over)
    return {"predictions": pred}


def test_predictions_are_validated(tmp_path):
    p = tmp_path / ds.PREDICTIONS_FILENAME
    p.write_text(json.dumps(_pred_doc()))
    pred, sha = ds.load_predictions(p, typologies=["gather_scatter"])
    import hashlib

    assert sha == hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="no prediction"):
        ds.load_predictions(p, typologies=["gather_scatter", "stack"])
    bad = _pred_doc(typologies={"gather_scatter": {"predicted_ap": 0.9, "pi": [0.6, 0.8]}})
    p.write_text(json.dumps(bad))
    with pytest.raises(ValueError, match="malformed"):
        ds.load_predictions(p)
    p.write_text(json.dumps(_pred_doc(generator_image="")))
    with pytest.raises(ValueError, match="generator_image"):
        ds.load_predictions(p)


def test_replication_reports_observed_against_the_interval():
    pred = _pred_doc()["predictions"]
    rep = ds.replication({"gather_scatter": {"ap": 0.85}}, pred)
    assert rep["gated"] is False
    assert rep["typologies"]["gather_scatter"]["inside_pi"] is False
    rep = ds.replication({"gather_scatter": {"ap": 0.65}}, pred)
    assert rep["typologies"]["gather_scatter"]["inside_pi"] is True


def test_level2_reports_the_original_four_beside_k_of_6():
    from lakebench.aml.fidelity_gate import _level2

    beh = PREREG["behavioural_subset"]
    in_band = {"gather_scatter", "dormant_reactivation", "micro_structuring", "corridor_high_risk"}
    per = {
        t: {"status": "ok", "in_band": t in in_band, "underpowered": False, "leakage_pass": True}
        for t in beh
    }
    out = _level2(per, PREREG)
    assert out["k_in_band"] == 4 and out["holds_on_this_corpus"] is True
    four = out["original_four"]
    assert four["gated"] is False and four["n"] == 4 and four["k_required"] == 3
    assert four["k_in_band"] == 2 and four["holds_on_this_corpus"] is False


def test_registered_look_needs_a_clean_checkout(monkeypatch):
    mod = _runner()

    class Done:
        def __init__(self, out):
            self.stdout = out

    monkeypatch.setattr(mod.subprocess, "run", lambda *a, **k: Done(" M src/x.json\n"))
    assert "clean checkout" in mod.clean_checkout_error()
    monkeypatch.setattr(mod.subprocess, "run", lambda *a, **k: Done(""))
    assert mod.clean_checkout_error() is None
