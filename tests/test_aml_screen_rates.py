"""scripts/aml_screen_rates.py (AM-6): W5/W6 non-planted alerts per customer.

TEST VALUES ONLY: records are copies of a stored seed-43 AML batch record
with the scorer's keys and an observed datagen block added; the calibration
seed is faked and the held-out seed comes from the test fixture. No real
held-out seed is used.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from lakebench.config import datagen_seed
from tests.conftest import exec_repo_script
from tests.fixtures import heldout_test_seeds as ts

ROOT = Path(__file__).resolve().parents[1]
RECORD = ROOT / "tests/fixtures/records/run-20260928-103055-de1772/metrics.json"
FAKE_CALIBRATION = 1_234_567
DIGEST = "sha256:" + "ab" * 32
W5, W6 = "W5_sanctions_match", "W6_pep_counterparty"


@pytest.fixture
def mod(monkeypatch):
    monkeypatch.setattr(datagen_seed, "calibration_seed", lambda: FAKE_CALIBRATION)
    return exec_repo_script(ROOT / "scripts/aml_screen_rates.py", "aml_screen_rates")


def _record(run: str, scale: float, w5: int, w6: int, customers: int, seed: int = 43) -> dict:
    rec = copy.deepcopy(json.loads(RECORD.read_text()))
    rec["run_id"] = run
    corpus = rec["experiment"]["corpus"]
    corpus["scale"] = scale
    corpus["seed"] = seed
    corpus["datagen"].update(observed=True, digest=DIGEST, scale=scale, seed=seed)
    fs = rec["financial_scoring"] = dict(rec.get("financial_scoring") or {})
    fs["nonplanted_alerts_by_rule"] = {"W2_structuring": 3, W5: w5, W6: w6}
    fs["customer_count"] = customers
    fs["evidence_capped_alerts_by_rule"] = {W5: 2} if scale == 1.0 else {}
    fs["rules"] = [
        {"rule_id": r, "status": "ran", "alert_count": 1} for r in (W5, W6, "W2_structuring")
    ]
    return rec


def _four() -> list[dict]:
    return [
        _record("s1-a", 1.0, 40, 20, 10_000),
        _record("s10-a", 10.0, 1_200, 400, 100_000),
        _record("s1-c", 1.0, 6, 50, 98_731, seed=FAKE_CALIBRATION),
        _record("s10-c", 10.0, 151, 600, 987_654, seed=FAKE_CALIBRATION),
    ]


def _about(problems: list[str], run: str) -> list[str]:
    return [p for p in problems if p.startswith(f"run {run}:")]


def test_rates_and_ratio_from_the_four_records(mod):
    doc, problems = mod.build(_four())
    assert problems == []
    by = {(f["seed_role"], f["scale"], f["rule"]): f for f in doc["figures"]}
    assert by[("seed-43", 1.0, W5)]["per_customer"] == 0.004
    assert by[("seed-43", 10.0, W5)]["per_customer"] == 0.012
    assert by[("seed-43", 10.0, W6)]["per_customer"] == 0.004
    assert all(f["n"] == 1 and f["run_id"] and f["generator"] == DIGEST for f in doc["figures"])
    assert by[("seed-43", 1.0, W5)]["evidence_capped_alerts"] == 2
    assert by[("seed-43", 1.0, W6)]["evidence_capped_alerts"] == 0
    ratios = {(r["seed_role"], r["rule"]): r for r in doc["ratios"]}
    assert ratios[("seed-43", W5)]["s10_over_s1"] == 3.0
    assert ratios[("seed-43", W6)]["s10_over_s1"] == 2.0
    assert ratios[("seed-43", W5)]["runs"] == ["s1-a", "s10-a"]
    # Labelled where an evidence cap cut either side, and only there.
    assert ratios[("seed-43", W5)]["bounded_by_evidence_cap"] is True
    assert ratios[("seed-43", W6)]["bounded_by_evidence_cap"] is False


def test_ratio_is_computed_from_raw_counts_not_rounded_rates(mod):
    doc, _ = mod.build(_four())
    ratios = {(r["seed_role"], r["rule"]): r for r in doc["ratios"]}
    # (151 / 987,654) / (6 / 98,731) = 2.5159..., while 6-dp rounded rates
    # would give 2.508.
    assert ratios[("calibration", W5)]["s10_over_s1"] == 2.516


def test_zero_at_scale1_leaves_the_ratio_undefined_with_a_reason(mod):
    records = _four()
    records[0]["financial_scoring"]["nonplanted_alerts_by_rule"][W6] = 0
    records[0]["financial_scoring"]["nonplanted_alerts_by_rule"][W5] = 60
    doc, problems = mod.build(records)
    assert problems == []
    ratio = next(r for r in doc["ratios"] if r["seed_role"] == "seed-43" and r["rule"] == W6)
    assert ratio["s10_over_s1"] is None and ratio["undefined_reason"]


def test_output_holds_no_seed_integer(mod, tmp_path):
    paths = []
    for i, rec in enumerate(_four()):
        p = tmp_path / f"r{i}" / "metrics.json"
        p.parent.mkdir()
        p.write_text(json.dumps(rec))
        paths.append(str(p.parent))
    out = tmp_path / "rates.json"
    assert mod.main([*paths, "--out", str(out)]) == 0
    text = out.read_text()
    assert str(FAKE_CALIBRATION) not in text
    assert "seed-43" in text and "calibration" in text
    assert '"seed"' not in text
    assert mod.main([*paths, "--out", str(out), "--check"]) == 0
    out.write_text(text.replace("calibration", "x", 1))
    assert mod.main([*paths, "--out", str(out), "--check"]) == 1


def test_a_missing_run_of_the_four_stops_the_file(mod, tmp_path):
    paths = []
    for i, rec in enumerate(_four()[:3]):
        p = tmp_path / f"r{i}.json"
        p.write_text(json.dumps(rec))
        paths.append(str(p))
    out = tmp_path / "rates.json"
    assert mod.main([*paths, "--out", str(out)]) == 2
    assert not out.exists()
    _doc, problems = mod.build(_four()[:3])
    assert problems == ["calibration: no usable record at scale 10"]


def test_protected_role_is_refused(mod):
    rec = _record("ev", 1.0, 40, 20, 10_000)
    rec["experiment"]["corpus"]["corpus_role"] = "evaluation"
    doc, problems = mod.build([rec, *_four()])
    assert len(_about(problems, "ev")) == 1 and "protected" in _about(problems, "ev")[0]
    assert "ev" not in {f["run_id"] for f in doc["figures"]}


def test_held_out_seed_is_refused_without_naming_it(mod, monkeypatch):
    ts.use_fixture(monkeypatch)
    rec = _record("ho", 1.0, 40, 20, 10_000, seed=ts.TEST_EVALUATION_SEED)
    _doc, problems = mod.build([rec])
    assert len(_about(problems, "ho")) == 1 and "protected" in _about(problems, "ho")[0]
    assert all(str(ts.TEST_EVALUATION_SEED) not in p for p in problems)


def test_unreadable_held_out_record_fails_closed(mod, monkeypatch):
    def broken():
        raise OSError("gone")

    monkeypatch.setattr(datagen_seed, "_heldout", broken)
    _doc, problems = mod.build([_record("s1-a", 1.0, 40, 20, 10_000)])
    assert len(_about(problems, "s1-a")) == 1 and "protected" in _about(problems, "s1-a")[0]


@pytest.mark.parametrize(
    "drop, why",
    [
        ("nonplanted_alerts_by_rule", "nonplanted_alerts_by_rule lacks W5 or W6"),
        ("customer_count", "customer_count is missing or not positive"),
        (
            "evidence_capped_alerts_by_rule",
            "evidence_capped_alerts_by_rule is missing or malformed",
        ),
    ],
)
def test_record_without_the_keys_is_refused_by_name(mod, drop, why):
    rec = _record("old", 1.0, 40, 20, 10_000)
    del rec["financial_scoring"][drop]
    _doc, problems = mod.build([rec])
    assert _about(problems, "old") == [f"run old: financial_scoring.{why}"]


def test_record_counting_other_rules_only_is_refused(mod):
    rec = _record("partial", 1.0, 40, 20, 10_000)
    del rec["financial_scoring"]["nonplanted_alerts_by_rule"][W6]
    _doc, problems = mod.build([rec])
    assert _about(problems, "partial") == [
        "run partial: financial_scoring.nonplanted_alerts_by_rule lacks W5 or W6"
    ]


@pytest.mark.parametrize("bad", [None, 55.9, -3, True, "40"])
def test_a_non_count_is_refused(mod, bad):
    rec = _record("odd", 1.0, 40, 20, 10_000)
    rec["financial_scoring"]["nonplanted_alerts_by_rule"][W5] = bad
    _doc, problems = mod.build([rec])
    assert _about(problems, "odd") == [
        "run odd: financial_scoring.nonplanted_alerts_by_rule holds a non-count for W5 or W6"
    ]


def test_a_rule_that_did_not_run_is_refused_not_read_as_zero(mod):
    rec = _record("skip", 1.0, 40, 0, 10_000)
    rec["financial_scoring"]["rules"][1]["status"] = "skipped"
    _doc, problems = mod.build([rec])
    assert _about(problems, "skip") == [
        "run skip: W6_pep_counterparty did not run (financial_scoring.rules)"
    ]


@pytest.mark.parametrize(
    "change, why",
    [
        ({"observed": False}, "no observed generator digest"),
        ({"digest": None}, "no observed generator digest"),
        ({"scale": 10.0}, "the observed datagen scale differs"),
        ({"seed": 44}, "the observed datagen seed is missing or differs"),
        ({"seed": None}, "the observed datagen seed is missing or differs"),
    ],
)
def test_a_corpus_without_observed_identity_is_refused(mod, change, why):
    rec = _record("old-data", 1.0, 40, 20, 10_000)
    rec["experiment"]["corpus"]["datagen"].update(change)
    _doc, problems = mod.build([rec])
    got = _about(problems, "old-data")
    assert len(got) == 1 and why in got[0]


def test_record_without_a_scale_is_refused(mod):
    rec = _record("noscale", 1.0, 40, 20, 10_000)
    rec["experiment"]["corpus"]["scale"] = None
    _doc, problems = mod.build([rec])
    assert _about(problems, "noscale") == ["run noscale: experiment.corpus.scale is missing"]


def test_failed_verdict_is_refused(mod):
    rec = _record("bad", 10.0, 40, 20, 10_000)
    rec["verdict"]["status"] = "FAILED"
    _doc, problems = mod.build([rec])
    got = _about(problems, "bad")
    assert len(got) == 1 and "verdict FAILED" in got[0]


@pytest.mark.parametrize("field", ["generator", "workload_version"])
def test_a_pair_from_different_generators_or_versions_is_refused(mod, field):
    records = _four()
    if field == "generator":
        records[1]["experiment"]["corpus"]["datagen"]["digest"] = "sha256:" + "cd" * 32
    else:
        records[1]["experiment"]["workload"]["version"] = "aml-2"
    _doc, problems = mod.build(records)
    assert problems == [
        f"runs s1-a and s10-a: seed-43 at scale 1 and 10 differ in {field}, "
        "so the ratio would not isolate scale"
    ]


def test_other_seed_continuous_and_duplicate_are_refused(mod):
    other = _record("other", 1.0, 40, 20, 10_000, seed=99)
    cont = _record("cont", 1.0, 40, 20, 10_000)
    cont["experiment"]["mode"] = "sustained"
    dup = _record("b", 1.0, 41, 20, 10_000)
    _doc, problems = mod.build([other, cont, *_four(), dup])
    assert any("run other" in p and "neither 43 nor the calibration" in p for p in problems)
    assert any("run cont" in p and "not an AML batch run" in p for p in problems)
    assert any("runs s1-a and b" in p for p in problems)
    assert len(problems) == 3


def test_too_few_scale1_alerts_stop_the_file(mod):
    records = _four()
    records[0]["financial_scoring"]["nonplanted_alerts_by_rule"].update({W5: 30, W6: 19})
    _doc, problems = mod.build(records)
    assert problems == [
        "seed-43: 49 non-planted W5 plus W6 alerts at scale 1, under 50: "
        "the ratio is too noisy to publish"
    ]


def test_another_scale_is_refused(mod):
    rec = _record("s100", 100.0, 40, 20, 10_000)
    _doc, problems = mod.build([rec, *_four()])
    assert problems == ["run s100: scale 100 is not one the figure uses (1 or 10)"]


@pytest.mark.parametrize(
    "path, value",
    [
        (("financial_scoring", "evidence_capped_alerts_by_rule"), ["W5_sanctions_match"]),
        (("financial_scoring",), []),
        (("experiment",), "x"),
        (("financial_scoring", "rules"), {"W5": "ran"}),
    ],
)
def test_malformed_shapes_are_refused_not_crashed(mod, path, value):
    rec = _record("shape", 1.0, 40, 20, 10_000)
    node = rec
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    _doc, problems = mod.build([rec])
    assert len(_about(problems, "shape")) == 1
