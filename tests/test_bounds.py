"""BOUNDED BY trickle and the bound-kind registry (EVD-4, DESIGN ch03 section 4).

SPEC section 8: a continuous run is BOUNDED BY trickle when a trickle was
set and the pipeline kept pace: ingested / offered rows >= 0.99 and lag at
window end within one trigger interval. The trickle is never a bound kind,
so no identity moves.
"""

from __future__ import annotations

import copy
import io
import re
from unittest import mock

import pytest

from lakebench.metrics import bounds
from lakebench.metrics.continuous_window import trickle_kept_pace
from tests.fixtures import stored_records as sr

#: The seven stored continuous exp1 records and their lag at window end,
#: window_seconds minus the bronze stream's last_write_offset_seconds, read
#: from each record (trigger 30 s, 20 s for a112be).
SEVEN = {
    "011043-e338c5": 0.7,
    "073533-9de9c9": 16.6,
    "073818-7934eb": 12.9,
    "095006-71b4a3": 10.5,
    "140552-a112be": 12.8,
    "204941-1d17f4": 20.6,
    "205000-ebb26f": 22.4,
}


def _rec(run_id: str = "011043-e338c5") -> dict:
    return sr.load_record(run_id)


def _scores(rec: dict) -> dict:
    return rec["pipeline_benchmark"]["scores"]


@pytest.mark.parametrize(("run_id", "lag"), sorted(SEVEN.items()))
def test_seven_stored_continuous_bounded(run_id, lag):
    tb = bounds.trickle_bound(_rec(run_id))
    assert tb is not None
    assert tb["kind"] == "trickle" and tb["kept_pace"] is True
    assert tb["lag_s"] == lag
    assert tb["source"] == "auto"
    assert tb["value"] == _rec(run_id)["config_snapshot"]["sustained"]["max_files_per_trigger"]
    assert tb["ratio"] >= 0.99


def test_record_and_run_agree():
    """The saved record and the run it was saved from give one answer."""
    for run_id in sr.record_ids():
        assert bounds.trickle_bound(sr.load_record(run_id)) == bounds.trickle_bound(
            sr.load_metrics(run_id)
        ), run_id


def test_batch_record_not_bounded():
    assert bounds.trickle_bound(_rec("231711-6dd3bc")) is None


def test_legacy_record_pace_not_measured():
    tb = bounds.trickle_bound(_rec("215221-65567b"))
    assert tb is not None and tb["kept_pace"] is None and tb["not_measured"]


def test_fell_behind_not_bounded():
    rec = _rec()
    _scores(rec)["ingest_ratio"] = 0.90
    assert bounds.trickle_bound(rec) is None


def test_no_trickle_not_bounded():
    rec = _rec()
    rec["continuous"]["trickle"]["value"] = None
    rec["config_snapshot"]["sustained"]["max_files_per_trigger"] = None
    rec["config_snapshot"]["experiment_inputs"].setdefault("config_limits", {})[
        "max_files_per_trigger"
    ] = None
    assert bounds.trickle_bound(rec) is None


def test_lag_over_one_trigger_not_bounded():
    rec = _rec()
    bronze = next(s for s in rec["streaming"] if s["job_type"] == "bronze-ingest")
    bronze["last_write_offset_seconds"] = 1800.0 - 31.0  # 30 s trigger
    assert bounds.trickle_bound(rec) is None
    bronze["last_write_offset_seconds"] = 1800.0 - 30.04  # within the 0.1 s rounding
    assert bounds.trickle_bound(rec)["kept_pace"] is True


def test_corpus_fallback_ratio_is_not_read_as_falling_behind():
    """Without released_rows the collector's ingest_ratio is the corpus
    share (0.76 here), which says nothing about pace."""
    rec = _rec()
    _scores(rec)["released_rows"] = None
    _scores(rec)["ingest_ratio"] = _scores(rec)["corpus_ingest_ratio"]
    tb = bounds.trickle_bound(rec)
    assert tb is not None and tb["kept_pace"] is None


def test_unparseable_trigger_pace_not_measured():
    rec = _rec()
    rec["config_snapshot"]["sustained"]["bronze_trigger_interval"] = "soon"
    tb = bounds.trickle_bound(rec)
    assert tb is not None and tb["kept_pace"] is None


def test_corpus_taken_skips_the_lag():
    """Bronze took the whole corpus early: nothing was left to offer, so a
    last write long before the window end is not falling behind."""
    rec = _rec()
    _scores(rec)["corpus_ingest_ratio"] = 1.0
    bronze = next(s for s in rec["streaming"] if s["job_type"] == "bronze-ingest")
    bronze["last_write_offset_seconds"] = 900.0
    tb = bounds.trickle_bound(rec)
    assert tb["kept_pace"] is True and tb["lag_s"] is None


def test_the_trigger_in_flight_is_not_counted_against_pace():
    """released_rows counts the trigger at the window edge; a batch of it in
    flight is within the one-interval lag the definition allows."""
    one_trigger = 2 * 2478560 / 160  # files per trigger x rows per file
    released = 1858920
    kept = trickle_kept_pace(
        ingested_rows=released - one_trigger,
        released_rows=released,
        rows_per_trigger=one_trigger,
        corpus_taken=False,
        window_s=1800.0,
        last_write_offset_s=1790.0,
        trigger_s=30.0,
    )
    assert kept["kept_pace"] is True and kept["ratio"] == 1.0
    behind = trickle_kept_pace(
        ingested_rows=released - 3 * one_trigger,
        released_rows=released,
        rows_per_trigger=one_trigger,
        corpus_taken=False,
        window_s=1800.0,
        last_write_offset_s=1790.0,
        trigger_s=30.0,
    )
    assert behind["kept_pace"] is False


# --- the experiment block ----------------------------------------------------


def _rebuilt(run_id: str):
    from lakebench.metrics.experiment import build_experiment

    m = sr.load_metrics(run_id)
    m.experiment = None
    return build_experiment(m)


def test_block_records_the_trickle_and_identity_does_not_move():
    from lakebench.metrics.experiment import identity_hash

    exp = _rebuilt("011043-e338c5")
    limits = exp["limits"]
    assert limits["trickle_bound"]["kept_pace"] is True
    assert limits["bound"][-1] == (
        "trickle: max_files_per_trigger 2 (auto), the pipeline kept pace"
    )
    assert "trickle" not in " ".join(limits["bound_kinds"])
    without = copy.deepcopy(exp)
    without["limits"].pop("trickle_bound")
    without["limits"]["bound"] = [
        b for b in without["limits"]["bound"] if not b.startswith(bounds.TRICKLE_LINE_PREFIX)
    ]
    assert identity_hash(exp) == identity_hash(without)


def test_batch_block_has_no_trickle():
    exp = _rebuilt("231711-6dd3bc")
    assert "trickle_bound" not in exp["limits"]


def test_stored_block_without_the_key_is_computed():
    rec = _rec()
    assert "trickle_bound" not in rec["experiment"]["limits"]
    assert bounds.record_trickle_bound(rec)["kept_pace"] is True
    rec["experiment"]["limits"]["trickle_bound"] = None
    assert bounds.record_trickle_bound(rec) is None


# --- bound kinds -------------------------------------------------------------


def _legacy_caps_bound(limits, rules):
    """experiment._caps_bound at integrate 9a77ab18, before bound_entries."""
    out = [
        f"{x['job_type']}: executor cap {x['cap']} (scale asks for {x['scale_derived']})"
        for x in limits.get("executors") or []
        if x.get("cap_hit")
    ]
    out += [
        f"{x['job_type']}: concurrent executor budget granted {x['budget_cap']['granted']} "
        f"of {x['budget_cap']['requested']}"
        for x in limits.get("executors") or []
        if x.get("budget_cap")
    ]
    if limits.get("tm_alerts_over_capacity"):
        out.append(
            f"TM max_alerts_per_customer ({limits.get('tm_max_alerts_per_customer')}): "
            f"{limits['tm_alerts_over_capacity']} alerts over capacity"
        )
    out += [f"auto-sizing: {c}" for c in limits.get("autosize_cuts") or []]
    if limits.get("maintenance_stopped"):
        out.append("pre-benchmark maintenance stopped on its time budget")
    for rule, why in (rules.get("skipped") or {}).items():
        if "cap" in str(why):
            out.append(f"rule {rule} skipped: {why}")
    return out


FULL_LIMITS = {
    "executors": [
        {"job_type": "silver-build", "cap": 28, "scale_derived": 40, "cap_hit": True},
        {
            "job_type": "silver-stream",
            "cap": 20,
            "scale_derived": 4,
            "budget_cap": {"granted": 2, "requested": 4},
        },
    ],
    "tm_alerts_over_capacity": 7,
    "tm_max_alerts_per_customer": 3,
    "autosize_cuts": ["silver-build executors 40 -> 28"],
    "maintenance_stopped": True,
}
FULL_RULES = {"skipped": {"W3": "rule cap 500 alerts", "W9": "giant-component"}}


def test_bound_kind_registered():
    """Every kind the limits can produce is in BOUND_KINDS, and the display
    lines are exactly what _caps_bound wrote before."""
    from lakebench.metrics.experiment import _bound_kinds, _caps_bound

    entries = bounds.bound_entries(FULL_LIMITS, FULL_RULES)
    assert len(entries) == 6
    for kind, _line in entries:
        assert bounds.is_registered(kind), kind
    assert _caps_bound(FULL_LIMITS, FULL_RULES) == _legacy_caps_bound(FULL_LIMITS, FULL_RULES)
    assert _bound_kinds(FULL_LIMITS, FULL_RULES) == [
        "TM max_alerts_per_customer",
        "auto-sizing cuts",
        "pre-benchmark maintenance budget",
        "rule W3 cap",
        "silver-build: executor cap",
        "silver-stream: concurrent executor budget",
    ]
    assert not bounds.is_registered("datagen: rate limit")


def test_an_unregistered_kind_fails(monkeypatch):
    monkeypatch.setattr(bounds, "BOUND_KINDS", bounds.BOUND_KINDS[1:])  # drop executor cap
    with pytest.raises(ValueError, match="not in BOUND_KINDS"):
        bounds.bound_entries(FULL_LIMITS, FULL_RULES)


def test_stored_blocks_reproduce_their_bound_lists():
    """Rebuilding each stored record's limits through bound_entries gives
    the bound and bound_kinds the record stored (before the trickle line)."""
    for run_id in sr.record_ids():
        exp = (sr.load_record(run_id).get("experiment") or {}).get("limits")
        if not exp:
            continue
        rebuilt = _rebuilt(run_id)["limits"]
        assert rebuilt["bound_kinds"] == exp.get("bound_kinds", []), run_id
        assert [
            b for b in rebuilt["bound"] if not b.startswith(bounds.TRICKLE_LINE_PREFIX)
        ] == exp.get("bound", []), run_id


# --- readers -----------------------------------------------------------------


def _rows(a: dict, b: dict) -> dict[str, dict]:
    from lakebench.cli._compare import _build_comparison

    return {r["metric"]: r for r in _build_comparison("A", a, "B", b)["metrics"]}


def test_p2_throughput_capped_by_the_trickle_qph_not():
    """Pinned pair P2 (C360 continuous, Trino vs Thrift): rows/s is BOUNDED
    BY trickle on both sides; QpH, freshness and the degradation are not."""
    rows = _rows(_rec("011043-e338c5"), _rec("073533-9de9c9"))
    for metric in ("sustained_throughput_rps", "pipeline_throughput_gb_per_second"):
        assert rows[metric]["capped"] is True, metric
    for metric in ("composite_qph", "data_freshness_seconds", "qph_degradation_pct"):
        assert rows[metric]["capped"] is False, metric


def test_p2_rendered_capped_token_on_throughput_only():
    from rich.console import Console

    from lakebench.cli import _compare

    buf = io.StringIO()
    a, b = _rec("011043-e338c5"), _rec("073533-9de9c9")
    with mock.patch.object(_compare, "console", Console(file=buf, width=250)):
        _compare._print_comparison_table(_compare._build_comparison("A", a, "B", b))
    text = buf.getvalue()
    rps = next(line for line in text.splitlines() if "sustained_throughput_rps" in line)
    qph = next(line for line in text.splitlines() if "composite_qph " in line)
    assert "capped" in rps and "capped" not in qph


def test_another_cap_still_caps_every_row():
    a, b = _rec("011043-e338c5"), _rec("073533-9de9c9")
    a["experiment"]["limits"]["bound_kinds"] = ["silver-stream: concurrent executor budget"]
    rows = _rows(a, b)
    assert rows["composite_qph"]["capped"] is True


def test_report_labels_intake_cards_only():
    from lakebench.reports.formatter import caps_bound_from, trickle_caps_from
    from lakebench.reports.generator import ReportGenerator

    m = sr.load_metrics("011043-e338c5")
    assert caps_bound_from(m) == []  # the trickle does not bound every number
    (label,) = trickle_caps_from(m)
    assert "offered load, not infrastructure capacity" in label
    html = ReportGenerator(output_dir="/nonexistent")._generate_sustained_summary(m)
    cards = re.findall(r'<div class="card">(.*?)</div>\s*</div>', html, re.S)
    by_label = {re.search(r'card-label">(.*?)<', c).group(1): c for c in cards if "card-label" in c}
    assert "BOUNDED BY" in by_label["Sustained Throughput"]
    assert "BOUNDED BY" in by_label["Compute Efficiency"]
    assert "BOUNDED BY" not in by_label["In-Stream QpH"]
    assert "BOUNDED BY" not in by_label["Data Freshness"]
