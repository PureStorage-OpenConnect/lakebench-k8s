"""One source of metric metadata (EVD-2, DESIGN ch03 section 2).

The report and the collector's
score_descriptions read unit, direction and band from
``metrics/metric_registry.py``. A score emitted with no entry fails here.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

import pytest

from lakebench.metrics import metric_registry as reg
from tests.fixtures import stored_records as sr


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


# --- named directions --------------------------------------------------------


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
    ("qph_degradation_withheld", "sustained"),
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
    assert reg.direction_hint(key, mode) == f"{direction} is better"


@pytest.mark.parametrize(("key", "mode"), NOT_DIRECTIONAL)
def test_scores_without_a_better_side(key, mode):
    assert not reg.is_directional(key, mode)
    assert reg.direction_hint(key, mode) == ""


@pytest.mark.parametrize(
    ("key", "mode", "band"),
    [
        ("qph_spread", "batch", "diagnostic"),
        ("maintenance_value_pct", "batch", "diagnostic"),
        ("compaction_ratio", "batch", "diagnostic"),
        ("benchmark_rounds_count", "sustained", "diagnostic"),
        ("window_seconds", "sustained", "config_bound"),
        ("ingest_ratio", "sustained", "guard"),
        ("scale_ratio", "batch", "correctness"),
    ],
)
def test_a_score_without_a_better_side_keeps_its_band(key, mode, band):
    assert reg.lookup(key, mode).band == band


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


def test_every_emitted_key_registered():
    """Every key _scores_dict can emit, and every score key in the pinned
    records and their rebuilt pipeline_benchmark, resolves in the registry,
    in its mode."""
    from lakebench.metrics.collector import build_pipeline_benchmark

    missing: list[str] = []

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
    assert stored_keys
    assert missing == []
