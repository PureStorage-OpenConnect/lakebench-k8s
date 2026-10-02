"""One source of metric metadata (EVD-2, DESIGN ch03 section 2).

compare, reproduce, the perf gate, the report and the collector's
score_descriptions read unit, direction and band from
``metrics/metric_registry.py``. A score emitted with no entry fails here.
"""

from __future__ import annotations

import ast
import inspect
import io
import json
import textwrap
from pathlib import Path
from unittest import mock

import pytest

from lakebench.metrics import metric_registry as reg
from tests.fixtures import stored_records as sr

EXPECTED_DESCRIPTIONS = Path(__file__).parent / "expected" / "score_descriptions.json"


def _mode(raw: dict) -> str | None:
    return reg.canonical_mode((raw.get("pipeline_benchmark") or {}).get("pipeline_mode"))


# --- every emitted key is registered ----------------------------------------


def _scores_dict_keys() -> dict[str, set[str]]:
    """Top-level score keys ``PipelineBenchmark._scores_dict`` can emit, by
    mode, read from its source: string subscripts of ``scores`` and
    ``batch_scores`` and the keys of the dict literals assigned to them. A
    key in the ``pipeline_mode == "sustained"`` branch is continuous."""
    from lakebench.metrics.collector import PipelineBenchmark

    tree = ast.parse(textwrap.dedent(inspect.getsource(PipelineBenchmark._scores_dict)))
    out: dict[str, set[str]] = {"batch": set(), "sustained": set()}
    names = {"scores", "batch_scores"}

    def dict_keys(d: ast.Dict) -> set[str]:
        """Top-level keys of a dict literal, through ``**{...}`` splats (an
        expression splat is read from the rebuilt records instead)."""
        keys: set[str] = set()
        for k, v in zip(d.keys, d.values, strict=True):
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.add(k.value)
            elif k is None:
                for inner in ast.walk(v):
                    if isinstance(inner, ast.Dict):
                        keys |= {
                            x.value
                            for x in inner.keys
                            if isinstance(x, ast.Constant) and isinstance(x.value, str)
                        }
        return keys

    def collect(node: ast.AST, mode: str) -> None:
        if isinstance(node, ast.If) and "pipeline_mode" in ast.unparse(node.test):
            for child in node.body:
                collect(child, "sustained")
            for child in node.orelse:
                collect(child, mode)
            return
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                if (
                    isinstance(t, ast.Subscript)
                    and isinstance(t.value, ast.Name)
                    and t.value.id in names
                    and isinstance(t.slice, ast.Constant)
                ):
                    out[mode].add(t.slice.value)
                if isinstance(t, ast.Name) and t.id in names and isinstance(node.value, ast.Dict):
                    out[mode] |= dict_keys(node.value)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("update", "setdefault")
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in names
        ):
            for arg in node.args:
                if isinstance(arg, ast.Dict):
                    out[mode] |= dict_keys(arg)
                elif isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    out[mode].add(arg.value)
        for child in ast.iter_child_nodes(node):
            collect(child, mode)

    for stmt in tree.body[0].body:  # type: ignore[attr-defined]
        collect(stmt, "batch")
    return out


def test_every_emitted_key_registered():
    """(a) every key _scores_dict can emit, in the mode it emits it; (b)
    every score key in the pinned records and in their rebuilt
    pipeline_benchmark; (c) every number reproduce and the perf gate take
    from those records. Each resolves, in its mode."""
    from lakebench.cli._reproduce import _extract_expected_numbers
    from lakebench.metrics.collector import build_pipeline_benchmark
    from lakebench.metrics.perf_gate import RunRecord, extract_metrics

    missing: list[str] = []
    for mode, keys in _scores_dict_keys().items():
        for key in sorted(keys):
            meta = reg.lookup(key, mode)
            if meta is None or mode not in meta.modes:
                missing.append(f"{key} ({mode}, _scores_dict)")
    assert len(_scores_dict_keys()["sustained"]) > 20 and len(_scores_dict_keys()["batch"]) > 20

    stored_keys: set[str] = set()
    for run_id in sr.record_ids():
        raw = sr.load_record(run_id)
        mode = _mode(raw)
        stored = ((raw.get("pipeline_benchmark") or {}).get("scores")) or {}
        stored_keys |= set(stored)
        for key in stored:
            if reg.lookup(key, mode) is None:
                missing.append(f"{key} ({run_id} stored)")
        metrics = sr.load_metrics(run_id)
        if metrics.pipeline_benchmark is not None:
            rebuilt = build_pipeline_benchmark(metrics)._scores_dict()
            for key in rebuilt:
                if reg.lookup(key, mode) is None:
                    missing.append(f"{key} ({run_id} rebuilt)")
        record = RunRecord(run_id, sr.record_path(run_id), raw, metrics)
        numbers = set(_extract_expected_numbers(metrics))
        if raw.get("pipeline_benchmark"):
            numbers |= set(extract_metrics(record)[0])
        for key in numbers:
            if reg.lookup(key, mode) is None:
                missing.append(f"{key} ({run_id} reproduce/perf gate)")
    assert len(stored_keys) == 55  # the pinned records' score keys (design 03 section 2)
    assert missing == []


def test_an_unregistered_score_fails_the_drift_check(monkeypatch):
    """A new key in a dict literal, through ``.update`` under a condition,
    or inside the continuous ``**{...}`` splat is seen by the scan."""
    from lakebench.metrics import collector

    base = inspect.getsource(collector.PipelineBenchmark._scores_dict)
    src = base.replace('"time_to_value_seconds": round(', '"time_to_victory_seconds": round(')
    src = src.replace(
        "        # Batch scores\n",
        "        # Batch scores\n        if self.maintenance_stopped:\n"
        '            batch_scores.update({"maint_victory": 1})\n',
        1,
    )
    assert src != base
    monkeypatch.setattr(inspect, "getsource", lambda _obj: src)
    keys = _scores_dict_keys()
    assert "time_to_victory_seconds" in keys["batch"]
    assert "maint_victory" in keys["batch"]
    assert "composite_qph_rounds" in keys["sustained"]  # inside a ** splat
    assert reg.lookup("time_to_victory_seconds", "batch") is None


def test_continuous_maintenance_scores_are_registered_for_continuous():
    keys = _scores_dict_keys()["sustained"]
    for key in ("maintenance_elapsed_seconds", "compaction_ratio", "storage_reclaimed_mb"):
        assert key in keys
        assert "sustained" in reg.lookup(key, "sustained").modes


# --- named directions --------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "mode", "direction", "band"),
    [
        ("qph_degradation_pct", "sustained", "lower", "performance"),
        ("qph_spread", "batch", "none", "diagnostic"),
        ("maintenance_value_pct", "batch", "none", "diagnostic"),
        ("compaction_ratio", "batch", "higher", "diagnostic"),
        ("window_seconds", "sustained", "none", "config_bound"),
        ("benchmark_rounds_count", "sustained", "none", "diagnostic"),
        ("ingest_ratio", "sustained", "target", "guard"),
        ("scale_ratio", "batch", "target", "correctness"),
    ],
)
def test_named_directions(key, mode, direction, band):
    meta = reg.lookup(key, mode)
    assert (meta.direction, meta.band) == (direction, band)


#: Every directional score, by mode, with its direction; and the keys whose
#: compare colour changed with the registry (the old name-token rule gave
#: "higher" for qph/throughput/efficiency/rows_processed/ingest_ratio and
#: "lower" for everything else not on its neutral list).
DIRECTIONAL = [
    ("total_core_hours", "batch", "lower"),
    ("compute_efficiency_gb_per_core_hour", "batch", "higher"),
    ("compute_efficiency_gb_per_core_hour", "sustained", "higher"),
    ("pipeline_throughput_gb_per_second", "batch", "higher"),
    ("pipeline_throughput_gb_per_second", "sustained", "higher"),
    ("total_elapsed_seconds", "batch", "lower"),
    ("composite_qph", "batch", "higher"),
    ("composite_qph", "sustained", "higher"),
    ("data_freshness_seconds", "sustained", "lower"),
    ("sustained_throughput_rps", "sustained", "higher"),
    ("time_to_detect_seconds", "sustained", "lower"),
    ("time_to_detect_p95_seconds", "sustained", "lower"),
    ("time_to_detect_max_seconds", "sustained", "lower"),
    ("in_stream_composite_qph", "sustained", "higher"),
    ("qph_degradation_pct", "sustained", "lower"),
    ("time_to_value_seconds", "batch", "lower"),
    ("maintenance_elapsed_seconds", "batch", "lower"),
    ("maintenance_elapsed_seconds", "sustained", "lower"),
    ("pre_compaction_qph", "batch", "higher"),
    ("post_compaction_qph", "batch", "higher"),
]
NOT_DIRECTIONAL = [
    ("total_core_hours", "sustained"),
    ("total_elapsed_seconds", "sustained"),
    ("total_data_processed_gb", "batch"),
    ("composite_qph_rounds", "sustained"),
    ("window_seconds", "sustained"),
    ("arrival_seconds", "sustained"),
    ("window_arrival_fraction", "sustained"),
    ("pre_window_rows", "sustained"),
    ("ingest_ratio", "sustained"),
    ("corpus_ingest_ratio", "sustained"),
    ("released_rows", "sustained"),
    ("corpus_drain_seconds", "sustained"),
    ("bronze_busy_fraction", "sustained"),
    ("time_to_detect_alerts", "sustained"),
    ("time_to_detect_late_alerts", "sustained"),
    ("time_to_detect_unmeasured_cycles", "sustained"),
    ("total_rows_processed", "sustained"),
    ("total_s3_objects", "sustained"),
    ("query_time_event_age_seconds", "sustained"),
    ("benchmark_rounds_count", "sustained"),
    ("scale_ratio", "batch"),
    ("maintenance_pct_of_pipeline", "batch"),
    ("pre_compaction_file_count", "batch"),
    ("post_compaction_file_count", "batch"),
    ("compaction_ratio", "batch"),
    ("snapshots_expired", "batch"),
    ("orphan_files_removed", "batch"),
    ("storage_reclaimed_mb", "batch"),
    ("maintenance_value_pct", "batch"),
    ("benchmark_samples_per_query", "batch"),
    ("maintenance_paired_queries", "batch"),
    ("maintenance_settle_seconds", "batch"),
]


@pytest.mark.parametrize(("key", "mode", "direction"), DIRECTIONAL)
def test_directional_scores(key, mode, direction):
    assert reg.is_directional(key, mode)
    assert reg.higher_is_better(key, mode) is (direction == "higher")


@pytest.mark.parametrize(("key", "mode"), NOT_DIRECTIONAL)
def test_scores_without_a_better_side(key, mode):
    assert not reg.is_directional(key, mode)


def test_every_score_is_classified_here():
    """Each emitted score is in one of the two lists above in each mode it
    is emitted in, so a band change cannot slip by."""
    listed = {(k, m) for k, m, _d in DIRECTIONAL} | set(NOT_DIRECTIONAL)
    numeric_unlisted = []
    for mode, keys in _scores_dict_keys().items():
        for key in keys:
            meta = reg.lookup(key, mode)
            if meta.unit in ("bool", "text", "struct"):
                continue
            if (key, mode) not in listed and reg.is_directional(key, mode):
                numeric_unlisted.append((key, mode))
    assert numeric_unlisted == []


def test_only_performance_metrics_have_a_better_side():
    assert reg.direction_hint("qph_degradation_pct", "sustained") == "lower is better"
    for key, mode in [
        ("compaction_ratio", "batch"),
        ("ingest_ratio", "sustained"),
        ("total_rows_processed", "sustained"),
        ("benchmark_samples_per_query", "batch"),
    ]:
        assert not reg.is_directional(key, mode), key
        assert reg.direction_hint(key, mode) == ""
    assert reg.lookup("ingest_ratio", "sustained").guard_range == (0.95, 1.05)


def test_stage_seconds_by_mode():
    """Continuous stream stages are named bronze/silver/gold too, and run
    for the whole window; datagen and the post-window query stage are real
    times in either mode."""
    assert reg.lookup("silver_seconds", "batch").band == "performance"
    assert reg.lookup("silver_seconds", "sustained").band == "config_bound"
    assert not reg.is_directional("silver_seconds", "sustained")
    assert reg.lookup("query_seconds", "sustained").band == "performance"
    assert reg.lookup("datagen_seconds", "sustained").band == "performance"
    assert reg.lookup("total_core_hours", "sustained").band == "config_bound"
    assert reg.lookup("total_core_hours", "batch").band == "performance"
    assert reg.lookup("total_elapsed_seconds", "sustained").band == "diagnostic"
    assert reg.is_directional("compute_efficiency_gb_per_core_hour", "sustained")


def test_a_mode_split_key_needs_the_mode():
    with pytest.raises(reg.ModeRequired):
        reg.lookup("total_core_hours", None)
    with pytest.raises(reg.ModeRequired):
        reg.lookup("silver_seconds", None)
    assert reg.lookup("composite_qph_rounds", None).band == "diagnostic"
    # Split only by caps: answered, with every mode's caps.
    qph = reg.lookup("composite_qph", None)
    assert qph.directional and reg.BOUND_MAINTENANCE in qph.cap_dependence
    # Readers that only colour treat it as having no better side.
    assert not reg.is_directional("total_core_hours", None)


def test_alias_resolves_to_the_renamed_key():
    old = reg.lookup("query_time_freshness_seconds", "sustained")
    assert old == reg.lookup("query_time_event_age_seconds", "sustained")


def test_unknown_mode_is_refused():
    with pytest.raises(ValueError, match="unknown pipeline mode"):
        reg.lookup("composite_qph", "streaming")
    assert reg.canonical_mode("continuous") == "sustained"


# --- caps --------------------------------------------------------------------


def _bound_kinds(**limits) -> list[str]:
    """The bound kinds experiment._bound_kinds writes for these limits."""
    from lakebench.metrics.experiment import _bound_kinds

    return _bound_kinds(limits, {"skipped": limits.pop("skipped_rules", {})})


def test_each_bound_kind_caps_the_metrics_it_bounds():
    """Every kind experiment._bound_kinds writes, streaming job types
    included, caps the metrics it holds down; built from its own output so
    a renamed kind fails here."""
    cases = [
        (
            {"executors": [{"job_type": "silver-build", "cap_hit": True}]},
            ["time_to_value_seconds", "silver_seconds", "total_core_hours"],
            "batch",
        ),
        (
            {"executors": [{"job_type": "gold-finalize", "cap_hit": True}]},
            ["gold_seconds", "compute_efficiency_gb_per_core_hour"],
            "batch",
        ),
        (
            {"executors": [{"job_type": "bronze-verify", "budget_cap": {"granted": 4}}]},
            ["bronze_seconds", "pipeline_throughput_gb_per_second"],
            "batch",
        ),
        (
            {"skipped_rules": {"W3": "rule cap 500 alerts"}},
            ["time_to_value_seconds", "composite_qph", "query_qph_Q1"],
            "batch",
        ),
        ({"tm_alerts_over_capacity": True}, ["total_elapsed_seconds"], "batch"),
        ({"autosize_cuts": ["x"]}, ["total_core_hours"], "batch"),
        (
            {"maintenance_stopped": True},
            ["post_compaction_qph", "composite_qph", "query_qph_Q1", "maintenance_elapsed_seconds"],
            "batch",
        ),
        (
            {"executors": [{"job_type": "gold-refresh", "cap_hit": True}]},
            ["time_to_detect_seconds", "data_freshness_seconds", "total_core_hours"],
            "sustained",
        ),
        (
            {"skipped_rules": {"W3": "rule cap 500 alerts"}},
            [
                "time_to_detect_seconds",
                "data_freshness_seconds",
                "composite_qph",
                "in_stream_composite_qph",
                "query_qph_Q1",
            ],
            "sustained",
        ),
        (
            {"executors": [{"job_type": "bronze-ingest", "cap_hit": True}]},
            ["sustained_throughput_rps", "compute_efficiency_gb_per_core_hour"],
            "sustained",
        ),
    ]
    for limits, keys, mode in cases:
        kinds = _bound_kinds(**limits)
        assert len(kinds) == 1, (limits, kinds)
        for key in keys:
            assert reg.capped_by(key, kinds, mode) == kinds, (kinds, key)
    # A stage is capped by its own job's cap only, and correctness is never.
    silver = _bound_kinds(executors=[{"job_type": "silver-build", "cap_hit": True}])
    assert reg.capped_by("gold_seconds", silver, "batch") == []
    assert reg.capped_by("scale_ratio", silver, "batch") == []
    # Without a mode, against every mode's caps (a record with no mode).
    assert reg.capped_by("total_core_hours", ["auto-sizing cuts"], None) == ["auto-sizing cuts"]


def test_the_trickle_caps_intake_only():
    """The trickle is not a bound kind; a reader passes it as extra. It caps
    the throughputs, not freshness or time to detect."""
    assert reg.BOUND_TRICKLE == "trickle"
    for key in (
        "sustained_throughput_rps",
        "pipeline_throughput_gb_per_second",
        "compute_efficiency_gb_per_core_hour",
    ):
        assert reg.capped_by(key, [], "sustained", extra=["trickle"]) == ["trickle"], key
    for key in ("data_freshness_seconds", "time_to_detect_seconds", "composite_qph"):
        assert reg.capped_by(key, [], "sustained", extra=["trickle"]) == [], key


# --- reproduce and the perf gate keep their classification ------------------

#: What cli/_reproduce._classify_direction answered at integrate 5bcee1b4,
#: before the registry, for every metric reproduce or the perf gate reads.
LEGACY = {
    "scale_ratio": ("correctness", "exact"),
    "ingest_ratio": ("correctness", "exact"),
    "time_to_value_seconds": ("performance", "lower"),
    "data_freshness_seconds": ("performance", "lower"),
    "datagen_cpu_hr_per_tb": ("performance", "lower"),
    "pipeline_throughput_gb_per_second": ("performance", "higher"),
    "compute_efficiency_gb_per_core_hour": ("performance", "higher"),
    "composite_qph": ("performance", "higher"),
    "sustained_throughput_rps": ("performance", "higher"),
    "datagen_aggregate_mbps": ("performance", "higher"),
    "datagen_mbps_per_pod": ("performance", "higher"),
    "pre_compaction_qph": ("performance", "higher"),
    "query_qph_Q1_full_aggregation_scan": ("performance", "higher"),
    "bronze_seconds": ("performance", "lower"),
    "silver_seconds": ("performance", "lower"),
    "gold_seconds": ("performance", "lower"),
    "datagen_seconds": ("performance", "lower"),
    "query_seconds": ("performance", "lower"),
    "some_future_stage_seconds": ("performance", "lower"),
    "some_future_metric": ("performance", "exact"),
}

#: Keys whose classification moved with the registry, none of which
#: reproduce or the perf gate extracts (test below): an old exact or a
#: suffix-rule "lower" for a metric with or without a better side.
MOVED = {
    "in_stream_composite_qph": ("performance", "higher"),
    "post_compaction_qph": ("performance", "higher"),
    "qph_degradation_pct": ("performance", "lower"),
    "total_core_hours": ("performance", "lower"),
    "arrival_seconds": ("performance", "exact"),
    "corpus_drain_seconds": ("performance", "exact"),
    "maintenance_settle_seconds": ("performance", "exact"),
    "query_time_event_age_seconds": ("performance", "exact"),
    "query_time_freshness_seconds": ("performance", "exact"),
    "window_seconds": ("performance", "exact"),
}


def test_reproduce_classification_unchanged():
    from lakebench.cli._reproduce import _METRIC_TABLE, _classify_direction

    for key, want in LEGACY.items():
        assert _classify_direction(key) == want, key
    for key, want in MOVED.items():
        assert _classify_direction(key) == want, key
    assert {k: _METRIC_TABLE[k] for k in _METRIC_TABLE} == {k: LEGACY[k] for k in _METRIC_TABLE}


def test_moved_keys_never_reach_reproduce_or_the_perf_gate():
    from lakebench.cli._reproduce import _classify_direction, _extract_expected_numbers
    from lakebench.metrics.perf_gate import RunRecord, extract_metrics

    reached: set[str] = set()
    for run_id in sr.record_ids():
        raw = sr.load_record(run_id)
        metrics = sr.load_metrics(run_id)
        reached |= set(_extract_expected_numbers(metrics))
        if raw.get("pipeline_benchmark"):
            rec = RunRecord(run_id, sr.record_path(run_id), raw, metrics)
            reached |= set(extract_metrics(rec)[0])
    assert reached and not reached & set(MOVED)
    for key in reached:
        legacy = LEGACY.get(key) or (
            LEGACY["query_qph_Q1_full_aggregation_scan"] if key.startswith("query_qph_") else None
        )
        assert legacy is not None, key
        assert _classify_direction(key) == legacy, key


# --- descriptions and the report --------------------------------------------


def test_score_descriptions_unchanged():
    from lakebench.metrics.collector import PipelineBenchmark

    want = json.loads(EXPECTED_DESCRIPTIONS.read_text())["descriptions"]
    assert list(reg.descriptions().items()) == list(want.items())
    assert PipelineBenchmark._SCORE_DESCRIPTIONS == want


def _cards(html: str) -> list[str]:
    import re

    return re.findall(r'<div class="card-hint2">(.*?)</div>', html, re.S)


def test_report_cards_render_registry_hints():
    """The rendered summary cards carry the registry's hints: the batch TTV
    card lower, its throughput, efficiency and QpH cards higher; the
    continuous CPU-hours card none."""
    from lakebench.reports.generator import ReportGenerator

    gen = ReportGenerator(output_dir="/nonexistent")
    batch = _cards(gen._generate_batch_summary(sr.load_metrics("011123-497f02")))
    assert batch[0].startswith("&#8595; lower is better")  # time to value
    assert batch[1] == batch[2] == "&#8593; higher is better"  # throughput, efficiency
    assert batch[3].startswith("&#8593; higher is better")  # QpH
    cont_html = gen._generate_sustained_summary(sr.load_metrics("011043-e338c5"))
    cpu = next(c for c in _cards(cont_html) if "/day" in c)
    assert "better" not in cpu


def test_report_card_hints_match_the_registry():
    from lakebench.reports.generator import _direction_hint

    assert _direction_hint("time_to_value_seconds", "batch", "1m 2s") == (
        "&#8595; lower is better | 1m 2s"
    )
    assert _direction_hint("composite_qph", "batch") == "&#8593; higher is better"
    assert _direction_hint("sustained_throughput_rps", "sustained") == "&#8593; higher is better"
    # Continuous core-hours follow the window: the detail only.
    assert _direction_hint("total_core_hours", "sustained", "x/day") == "x/day"
    assert _direction_hint("compute_efficiency_gb_per_core_hour", "sustained") == (
        "&#8593; higher is better"
    )


# --- compare: stored pair P2 -------------------------------------------------


def _render(a: dict, b: dict) -> str:
    from rich.console import Console

    from lakebench.cli import _compare

    buf = io.StringIO()
    with mock.patch.object(_compare, "console", Console(file=buf, width=250)):
        _compare._print_comparison_table(_compare._build_comparison("A", a, "B", b))
    return buf.getvalue()


def _row(text: str, metric: str) -> str:
    return next(line for line in text.splitlines() if metric in line)


def test_p2_degraded_side_not_winner():
    """P2 (C360 continuous, Trino vs Thrift): qph_degradation_pct -7.85 on A
    and 26.97 on B. B degraded; its larger number was rendered as a B win
    when the direction came from the "qph" name token."""
    from lakebench.reports.formatter import DELTA_TOKEN_B_FASTER

    a, b = sr.load_record("011043-e338c5"), sr.load_record("073533-9de9c9")
    scores_a = a["pipeline_benchmark"]["scores"]
    scores_b = b["pipeline_benchmark"]["scores"]
    assert scores_a["qph_degradation_pct"] < scores_b["qph_degradation_pct"]
    assert reg.lookup("qph_degradation_pct", _mode(a)).direction == "lower"
    text = _render(a, b)
    row = _row(text, "qph_degradation_pct")
    assert DELTA_TOKEN_B_FASTER not in row
    assert "A_faster" in row  # coloured, for A: it degraded less
    # Continuous total elapsed and core-hours have no better side.
    for metric in ("total_elapsed_seconds", "total_core_hours"):
        assert metric in scores_a
        r = _row(text, metric)
        assert "A_faster" not in r and DELTA_TOKEN_B_FASTER not in r, r


def test_compare_does_not_colour_diagnostic_scores():
    a, b = sr.load_record("011043-e338c5"), sr.load_record("073533-9de9c9")
    text = _render(a, b)
    for metric in ("total_rows_processed", "bronze_busy_fraction"):
        assert metric in a["pipeline_benchmark"]["scores"]
        row = _row(text, metric)
        assert "a_faster" not in row.lower() and "b_faster" not in row.lower(), row
