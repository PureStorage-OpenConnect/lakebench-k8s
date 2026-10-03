"""EVD-3: requested and effective values are recorded and a mismatch is
labelled (metrics/requested_effective.py, ER-5).

Fixtures are pinned stored records (tests/fixtures/records) with named
fields edited. The LB-224 case: gold asked for nothing (auto) and ran
incremental, which aggregates only part of silver; before EVD-3 nothing in
the record said so. The LB-206 case: the trickle resolved automatically to
1 file per trigger; the record carries the entry and the BOUNDED BY label.
"""

from __future__ import annotations

import copy

import pytest

from lakebench.metrics import bounds
from lakebench.metrics import requested_effective as re_
from lakebench.metrics import verdict as V
from tests.fixtures import stored_records as sr

C360_BATCH = "5105a0"
AML_CONT = "ebb26f"
C360_CONT = "1d17f4"


def _metrics(rec: dict):
    from lakebench.metrics.storage import MetricsStorage

    return MetricsStorage()._dict_to_metrics(rec)


def _gold(rec: dict) -> dict:
    return [j for j in rec["jobs"] if j["job_type"] == "gold-finalize"][-1]


def _gold_reports(rec: dict, strategy: str, source: str) -> None:
    _gold(rec).setdefault("extra_metrics", {}).update(
        gold_strategy=strategy, gold_strategy_source=source
    )


def _requested(rec: dict, strategy: str) -> None:
    rec["config_snapshot"]["requested"] = {"gold_strategy": strategy}


def test_lb224_fixture_labelled() -> None:
    """Requested auto, effective incremental (not chosen for a cycle): the
    verdict stays PASSED and carries the label and a warning."""
    rec = sr.load_record(C360_BATCH)
    _requested(rec, "auto")
    _gold_reports(rec, "incremental", "auto")
    v = V.verdict_from_record(rec)
    assert v.status == "PASSED"
    label = v.qualifiers["requested_effective"]
    assert list(label) == ["gold_strategy"]
    assert label["gold_strategy"]["requested"] == "auto"
    assert label["gold_strategy"]["effective"] == "incremental"
    _ok, _reasons, warnings = V.compute_badge_status(_metrics(rec))
    assert "gold_strategy: requested auto, ran incremental (auto)" in warnings


def test_lb224_label_without_a_recorded_request() -> None:
    """A record from before the request was recorded: the script's source
    (auto) says no override reached it."""
    rec = sr.load_record(C360_BATCH)
    _gold_reports(rec, "incremental", "auto")
    assert "gold_strategy" in V.verdict_from_record(rec).qualifiers["requested_effective"]


def test_multi_cycle_incremental_gold_is_not_labelled() -> None:
    """CD-13: cycles 2+ of a multi-cycle run are incremental by design
    (source cycle), so no mismatch."""
    rec = sr.load_record(C360_BATCH)
    _requested(rec, "auto")
    first = copy.deepcopy(_gold(rec))
    first["extra_metrics"] = {"gold_strategy": "simple_agg", "gold_strategy_source": "auto"}
    rec["jobs"].insert(len(rec["jobs"]) - 1, first)
    _gold_reports(rec, "incremental", "cycle")
    entries = re_.derive(_metrics(rec))
    assert entries["gold_strategy[cycle=1]"]["effective"] == "simple_agg"
    assert entries["gold_strategy[cycle=2]"]["source"] == "cycle"
    assert re_.mismatches(entries) == []
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_override_that_reached_the_script_is_not_labelled() -> None:
    rec = sr.load_record(C360_BATCH)
    _requested(rec, "two_phase_agg")
    _gold_reports(rec, "two_phase_agg", "override")
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_override_that_did_not_reach_the_script_is_labelled() -> None:
    """The config named a strategy, the driver chose by size: labelled."""
    rec = sr.load_record(C360_BATCH)
    _requested(rec, "two_phase_agg")
    _gold_reports(rec, "simple_agg", "auto")
    label = V.verdict_from_record(rec).qualifiers["requested_effective"]
    assert label["gold_strategy"]["requested"] == "two_phase_agg"


def test_auto_simple_agg_is_not_labelled() -> None:
    rec = sr.load_record(C360_BATCH)
    _requested(rec, "auto")
    _gold_reports(rec, "simple_agg", "auto")
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_unrecorded_gold_strategy_claims_no_mismatch() -> None:
    """A record whose gold job logged no strategy (an older image)."""
    rec = sr.load_record(C360_BATCH)
    entry = re_.derive(_metrics(rec))["gold_strategy"]
    assert entry["effective"] == re_.NOT_RECORDED
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_lb206_fixture_labelled() -> None:
    """The trickle resolved automatically to 1: the entry is recorded and
    the throughput carries the BOUNDED BY trickle label (EVD-4)."""
    rec = sr.load_record(AML_CONT)
    assert rec["continuous"]["trickle"]["source"] == "auto"
    entry = re_.derive(_metrics(rec))["trickle"]
    assert (entry["requested"], entry["effective"], entry["source"]) == ("auto", 1, "auto")
    assert bounds.trickle_bound(rec)["kind"] == "trickle"
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_configured_trickle_is_requested_as_set() -> None:
    rec = sr.load_record(AML_CONT)
    rec["continuous"]["trickle"].update(value=3, source="config")
    entry = re_.derive(_metrics(rec))["trickle"]
    assert (entry["requested"], entry["effective"]) == (3, 3)
    assert re_.mismatches({"trickle": entry}) == []


def test_executor_override_run_with_fewer_is_labelled() -> None:
    rec = sr.load_record(C360_BATCH)
    entry = rec["experiment"]["limits"]["executors"][1]
    entry.update(override=8, observed=4)
    v = V.verdict_from_record(rec)
    key = f"executors[{entry['job_type']}]"
    assert v.status == "PASSED"
    assert v.qualifiers["requested_effective"][key]["requested"] == 8


def test_executor_replaced_mid_job_is_not_labelled() -> None:
    """The observed count counts every executor pod the operator listed, so
    a replaced executor reads one more than asked for: not a mismatch."""
    rec = sr.load_record(C360_BATCH)
    rec["experiment"]["limits"]["executors"][1].update(override=8, observed=9)
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_executor_profile_request_is_its_count() -> None:
    rec = sr.load_record("85b404")
    entries = re_.derive(_metrics(rec))
    e = next(v for k, v in entries.items() if k.startswith("executors[silver"))
    assert isinstance(e["requested"], int) and e["source"].startswith("profile")
    assert re_.mismatches(entries) == []


def test_capped_executors_name_the_cap() -> None:
    rec = sr.load_record(C360_BATCH)
    rec["experiment"]["limits"]["executors"][1].update(scale_derived=40, cap=28, cap_hit=True)
    e = list(re_.derive(_metrics(rec)).values())
    assert any(x["source"] == "profile, capped at 28" and x["requested"] == 28 for x in e)


def test_local_run_has_no_executor_entries() -> None:
    rec = sr.load_record(C360_BATCH)
    rec["config_snapshot"]["local"] = True
    assert not any(k.startswith("executors[") for k in re_.derive(_metrics(rec)))


def test_profile_count_under_a_budget_is_not_labelled_at_a_stream() -> None:
    """A concurrent budget below the profile is a Lakebench limit, labelled
    by limits.bound, not a request the run ignored."""
    rec = sr.load_record(C360_CONT)
    entry = rec["experiment"]["limits"]["executors"][1]
    entry.update(observed=2, budget_cap={"requested": 4, "granted": 2})
    entries = re_.derive(_metrics(rec))
    e = entries[f"executors[{entry['job_type']}]"]
    assert "concurrent budget granted 2" in e["source"]
    assert re_.mismatches(entries) == []


def test_mode_that_did_not_run_is_labelled() -> None:
    """Asked for continuous, the record holds batch jobs and no stream."""
    rec = sr.load_record(C360_BATCH)
    rec["config_snapshot"]["experiment_inputs"]["run_mode"] = "continuous"
    label = V.verdict_from_record(rec).qualifiers["requested_effective"]
    assert label["pipeline_mode"]["requested"] == "continuous"
    assert label["pipeline_mode"]["effective"] == "batch"


def test_run_that_stopped_before_any_stage_reads_not_recorded() -> None:
    """A continuous run that failed before its streams started: no stage in
    the record, so nothing says which pipeline ran (pipeline_benchmark's own
    mode says batch for it)."""
    rec = sr.load_record(C360_CONT)
    rec["streaming"], rec["jobs"] = [], []
    e = re_.derive(_metrics(rec))["pipeline_mode"]
    assert (e["requested"], e["effective"]) == ("continuous", re_.NOT_RECORDED)
    assert re_.mismatches({"pipeline_mode": e}) == []


def test_mode_from_the_command_line_is_named_as_such() -> None:
    rec = sr.load_record(C360_CONT)
    rec["config_snapshot"]["experiment_inputs"]["mode"] = "batch"
    e = re_.derive(_metrics(rec))["pipeline_mode"]
    assert (e["requested"], e["effective"], e["source"]) == (
        "continuous",
        "continuous",
        "command line",
    )


def test_a_mismatch_never_fails_and_never_enters_identity() -> None:
    from lakebench.metrics.experiment import identity_hash

    rec = sr.load_record(C360_BATCH)
    before = identity_hash(rec["experiment"])
    _requested(rec, "auto")
    _gold_reports(rec, "incremental", "auto")
    m = _metrics(rec)
    rec["experiment"]["requested_effective"] = re_.derive(m)
    rec["experiment"]["requested_effective_mismatches"] = ["gold_strategy"]
    assert identity_hash(rec["experiment"]) == before
    assert V.verdict_from_record(rec).status == "PASSED"


def test_stored_entries_are_read_as_stored_and_judged_now() -> None:
    """A record that stored its entries is not re-derived by later code, but
    which entries are mismatches is decided when it is read (a stored list
    is policy of its day and is ignored)."""
    rec = sr.load_record(C360_BATCH)
    _gold_reports(rec, "simple_agg", "auto")  # what re-deriving would read
    rec["experiment"]["requested_effective"] = {
        "gold_strategy": {
            "requested": "two_phase_agg",
            "effective": "simple_agg",
            "source": "auto",
            "stage": "gold-finalize",
        }
    }
    rec["experiment"]["requested_effective_mismatches"] = []
    label = V.verdict_from_record(rec).qualifiers["requested_effective"]
    assert label["gold_strategy"]["requested"] == "two_phase_agg"
    rec["experiment"]["requested_effective"]["gold_strategy"]["requested"] = "auto"
    rec["experiment"]["requested_effective_mismatches"] = ["gold_strategy"]
    assert "requested_effective" not in V.verdict_from_record(rec).qualifiers


def test_fresh_record_stores_the_entries() -> None:
    """A run of this version writes the entries into its experiment block,
    with the configured gold strategy from the snapshot."""
    m = sr.load_metrics(C360_BATCH)
    m.experiment = None  # a fresh run: the block is built at save
    m.config_snapshot = copy.deepcopy(m.config_snapshot)
    m.config_snapshot["requested"] = {"gold_strategy": "auto"}
    gold = [j for j in m.jobs if j.job_type == "gold-finalize"][-1]
    gold.extra_metrics = {"gold_strategy": "simple_agg", "gold_strategy_source": "auto"}
    d = m.to_dict()
    exp = d["experiment"]
    assert exp["requested_effective"]["gold_strategy"]["effective"] == "simple_agg"
    assert exp["requested_effective_mismatches"] == []
    assert exp["requested_effective"]["pipeline_mode"]["effective"] == "batch"
    assert any(k.startswith("executors[") for k in exp["requested_effective"])


@pytest.mark.parametrize("value,want", [(None, "auto"), ("two_phase_agg", "two_phase_agg")])
def test_snapshot_records_the_configured_gold_strategy(value, want) -> None:
    from lakebench.metrics.collector import build_config_snapshot
    from tests.conftest import make_config

    spark = {"conf": {"spark.lb.gold.strategy": value}} if value else {}
    cfg = make_config(
        architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}},
        **({"spark": spark} if spark else {}),
    )
    assert build_config_snapshot(cfg)["requested"] == {"gold_strategy": want}


def test_requested_gold_strategy_is_outside_the_perf_fingerprint() -> None:
    from lakebench.metrics.perf_gate import _FINGERPRINT_KEYS

    assert "requested" not in _FINGERPRINT_KEYS


def test_explicit_request_with_an_unrecorded_effective_claims_nothing() -> None:
    """An older script logged no strategy: the request cannot be judged."""
    rec = sr.load_record(C360_BATCH)
    _requested(rec, "two_phase_agg")
    entry = re_.derive(_metrics(rec))["gold_strategy"]
    assert entry["effective"] == re_.NOT_RECORDED
    assert re_.mismatches({"gold_strategy": entry}) == []


def test_override_seen_by_the_script_without_a_recorded_request() -> None:
    rec = sr.load_record(C360_BATCH)
    _gold_reports(rec, "two_phase_agg", "override")
    entry = re_.derive(_metrics(rec))["gold_strategy"]
    assert (entry["requested"], entry["effective"]) == ("two_phase_agg", "two_phase_agg")


@pytest.mark.parametrize("schema", ["financial", None])
def test_no_gold_entry_outside_customer360(schema) -> None:
    rec = sr.load_record(C360_BATCH)
    rec["config_snapshot"]["workload_schema"] = schema
    assert "gold_strategy" not in re_.derive(_metrics(rec))


def test_configured_gold_strategy_is_normalised() -> None:
    from lakebench.metrics.collector import build_config_snapshot
    from tests.conftest import make_config

    cfg = make_config(
        architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}},
        spark={"conf": {"spark.lb.gold.strategy": " Two_Phase_Agg "}},
    )
    assert build_config_snapshot(cfg)["requested"] == {"gold_strategy": "two_phase_agg"}


def test_a_malformed_stored_entry_does_not_break_the_verdict() -> None:
    rec = sr.load_record(C360_BATCH)
    rec["experiment"]["requested_effective"] = {
        "gold_strategy": {"requested": ["auto"], "effective": {"x": 1}, "source": "auto"}
    }
    assert V.verdict_from_record(rec).status == "PASSED"
