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
    # Ingested over released rows: the record's own ingest_ratio.
    assert tb["ratio"] == _scores(_rec(run_id))["ingest_ratio"]
    assert tb["offered_rows"] == _scores(_rec(run_id))["released_rows"]


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
    corpus = rec["pipeline_benchmark"]["config_snapshot"]["datagen_output_rows"]
    _scores(rec)["released_rows"] = corpus
    _scores(rec)["ingest_ratio"] = 1.0
    bronze = next(s for s in rec["streaming"] if s["job_type"] == "bronze-ingest")
    bronze["last_write_offset_seconds"] = 900.0
    tb = bounds.trickle_bound(rec)
    assert tb["kept_pace"] is True and tb["lag_s"] is None and tb["lag_note"]


@pytest.mark.parametrize(("ratio", "bound"), [(0.985, False), (0.9899, False), (0.99, True)])
def test_the_ratio_is_ingested_over_released(ratio, bound):
    """SPEC's 0.99 applies to ingested over released rows, as the record's
    ingest_ratio states it: one trigger short at 1800 s / 30 s (0.983) is
    not kept pace."""
    rec = _rec()
    _scores(rec)["ingest_ratio"] = ratio
    tb = bounds.trickle_bound(rec)
    assert (tb is not None) is bound
    if bound:
        assert tb["ratio"] == ratio and tb["offered_rows"] == _scores(rec)["released_rows"]


def test_intake_limit_trickle_rate_is_not_shown_as_capacity():
    """The collector says the trickle held intake (intake_limit trickle_rate)
    while the ratio is under 0.99: labelled, pace not shown either way."""
    rec = _rec()
    _scores(rec)["ingest_ratio"] = 0.93
    _scores(rec)["intake_limit"] = "trickle_rate"
    tb = bounds.trickle_bound(rec)
    assert tb is not None and tb["kept_pace"] is None and "intake_limit" in tb["not_measured"]


def test_an_unreadable_continuous_record_is_not_capacity(monkeypatch):
    monkeypatch.setattr(bounds, "trickle_bound", lambda _r: 1 / 0)
    tb = bounds.record_trickle_bound(_rec())
    assert tb is not None and tb["kept_pace"] is None
    assert bounds.record_trickle_bound(_rec("231711-6dd3bc")) is None


def test_not_measured_line_and_label():
    tb = bounds.trickle_bound(_rec("215221-65567b"))
    assert bounds.trickle_line(tb) == (
        "trickle: max_files_per_trigger 50; whether the pipeline kept pace was not measured"
    )
    assert bounds.trickle_label(tb) == (
        "trickle 50 files per trigger set; whether the pipeline kept pace was not measured"
    )


# --- the experiment block ----------------------------------------------------


def _rebuilt(run_id: str):
    from lakebench.metrics.experiment import build_experiment

    m = sr.load_metrics(run_id)
    m.experiment = None
    return build_experiment(m)


@pytest.mark.parametrize("run_id", sorted(SEVEN))
def test_block_records_the_trickle_and_identity_does_not_move(run_id):
    from lakebench.metrics.experiment import identity_hash

    exp = _rebuilt(run_id)
    limits = exp["limits"]
    value = _rec(run_id)["config_snapshot"]["sustained"]["max_files_per_trigger"]
    assert limits["trickle_bound"]["kept_pace"] is True
    assert limits["bound"][-1] == (
        f"trickle: max_files_per_trigger {value} (auto), the pipeline kept pace"
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
        bounds.bound_entries(FULL_LIMITS, FULL_RULES, strict=True)
    # A run saving its record never fails on it.
    assert len(bounds.bound_entries(FULL_LIMITS, FULL_RULES)) == 6


def test_every_bound_kind_caps_some_metric():
    """A registered kind that caps no metric would leave the numbers it
    holds down looking like capacity once readers cap per metric."""
    from lakebench.metrics import metric_registry as reg

    for kind in bounds.BOUND_KINDS:
        concrete = kind.name.replace("*", "silver-build" if ":" in kind.name else "W3")
        hits = [
            key
            for key, entries in reg.METRICS.items()
            for e in entries
            if reg.capped_by(key, [concrete], next(iter(e.modes)))
        ]
        if kind.name == bounds.BOUND_EXECUTOR_OVERRIDE:
            # Its writer and its metric reach come with the config override
            # work; until then nothing binds it.
            continue
        assert hits, kind.name


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


def test_one_side_trickle_bound_caps_the_row():
    a, b = _rec("011043-e338c5"), _rec("073533-9de9c9")
    b["experiment"]["limits"]["trickle_bound"] = None  # stored: B was not bound
    rows = _rows(a, b)
    assert rows["sustained_throughput_rps"]["capped"] is True
    assert rows["corpus_drain_seconds"]["capped"] is True


def test_another_cap_still_caps_every_row():
    a, b = _rec("011043-e338c5"), _rec("073533-9de9c9")
    a["experiment"]["limits"]["bound_kinds"] = ["silver-stream: concurrent executor budget"]
    rows = _rows(a, b)
    assert rows["composite_qph"]["capped"] is True


def _cards(html: str) -> dict[str, str]:
    cards = re.findall(r'<div class="card">(.*?)</div>\s*</div>', html, re.S)
    return {re.search(r'card-label">(.*?)<', c).group(1): c for c in cards if "card-label" in c}


def test_report_labels_intake_cards_only():
    from lakebench.reports.formatter import caps_bound_from, trickle_caps_from
    from lakebench.reports.generator import ReportGenerator

    m = sr.load_metrics("011043-e338c5")
    assert caps_bound_from(m) == []  # the trickle does not bound every number
    (label,) = trickle_caps_from(m)
    assert label == (
        "trickle 2 files per trigger; this is the offered load, not infrastructure capacity"
    )
    gen = ReportGenerator(output_dir="/nonexistent")
    html = gen._generate_sustained_summary(m)
    by_label = _cards(html)
    assert "BOUNDED BY" in by_label["Sustained Throughput"]
    assert "BOUNDED BY" in by_label["Compute Efficiency"]
    assert "BOUNDED BY" not in by_label["In-Stream QpH"]
    assert "BOUNDED BY" not in by_label["Data Freshness"]
    avg = re.search(
        r"Avg Pipeline Throughput:(.*?)</span>\s*</span>|Avg Pipeline Throughput:(.*?)\n",
        html,
        re.S,
    )
    assert avg and "BOUNDED BY" in (avg.group(0))
    pipeline = gen._generate_pipeline_benchmark_section(m)
    summary = re.search(r"Throughput: <strong>(.*?)</strong>", pipeline, re.S).group(1)
    assert "BOUNDED BY" in summary


def test_report_on_a_new_block_keeps_the_trickle_off_other_numbers():
    """A block built with the trickle line: caps_bound_from leaves it out,
    the cap count and the experiment section show it once."""
    from lakebench.reports.formatter import caps_bound_from
    from lakebench.reports.generator import ReportGenerator

    m = sr.load_metrics("011043-e338c5")
    m.experiment = _rebuilt("011043-e338c5")
    assert any(b.startswith("trickle:") for b in m.experiment["limits"]["bound"])
    assert caps_bound_from(m) == []
    assert len(caps_bound_from(m, include_trickle=True)) == 1
    assert bounds.binding_caps(m) == [
        "trickle: max_files_per_trigger 2 (auto), the pipeline kept pace"
    ]
    gen = ReportGenerator(output_dir="/nonexistent")
    section = gen._generate_experiment_section(m)
    assert section.count("trickle: max_files_per_trigger 2") == 1
    stored = sr.load_metrics("011043-e338c5")  # block from before the field
    assert gen._generate_experiment_section(stored).count("trickle: max_files_per_trigger 2") == 1


def test_cli_rows_per_second_carries_the_note():
    m = sr.load_metrics("011043-e338c5")
    assert bounds.trickle_note(m) == " (BOUNDED BY trickle: offered load, not capacity)"
    assert bounds.trickle_note(sr.load_metrics("231711-6dd3bc")) == ""
