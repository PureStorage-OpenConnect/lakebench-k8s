"""EVD-10 (ER-15): the batch alert-set fingerprint on the CLI side.

The driver line is parsed into the gold-finalize job, copied to
``experiment.results.alert_set``, compared as a result (ladder steps 2 and
5), and required on an exp2 AML batch record (step 4). The Spark side is
``tests/spark/test_alert_set_fingerprint.py``.
"""

from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

from lakebench.metrics import alert_set as als
from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import JobMetrics, MetricsCollector
from tests.test_comparability import SYSID, observe, series_body, two_nodes
from tests.test_experiment import _cfg, _metrics

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"


def _aset(**rules: tuple[int, str]) -> dict:
    by_rule = {r: {"rows": n, "h": h} for r, (n, h) in sorted(rules.items())}
    return {
        "spec": "as1",
        "columns": ["rule_id", "entity_id", "alert_ts"],
        "cols_sha": "0123456789abcdef",
        "rows": sum(v["rows"] for v in by_rule.values()),
        "h": str(sum(int(v["h"]) for v in by_rule.values())),
        "by_rule": by_rule,
    }


ASET = _aset(W1_connected_components=(3, "-12"), W2_structuring=(5, "907"))


def _line(body: dict, seconds: float | None = 1.25, prefix: str = "[lb] 2026-10-03T00:00:00 - "):
    out = dict(body)
    if seconds is not None:
        out["seconds"] = seconds
    return f"{prefix}LB_ALERT_SET {json.dumps(out, sort_keys=True)}"


# ---------------------------------------------------------------------------
# Parsing the driver line
# ---------------------------------------------------------------------------


class TestParse:
    def test_well_formed_line(self):
        got, secs, why = als.parse_alert_set("noise\n" + _line(ASET) + "\nmore noise\n")
        assert got == ASET and secs == 1.25 and why is None
        assert "seconds" not in got

    def test_last_line_wins(self):
        other = _aset(W2_structuring=(1, "5"))
        got, _s, _w = als.parse_alert_set(_line(ASET) + "\n" + _line(other))
        assert got == other

    def test_no_line_is_all_none(self):
        assert als.parse_alert_set("Gold finalize complete in 3.0s\n") == (None, None, None)
        assert als.parse_alert_set(None) == (None, None, None)

    def test_unavailable(self):
        got, secs, why = als.parse_alert_set(
            _line({"spec": "as1", "unavailable": "Table not found"}, seconds=0.1)
        )
        assert got is None and secs == 0.1 and why == "Table not found"

    @pytest.mark.parametrize(
        "mutate, problem",
        [
            (lambda b: b.update(rows=b["rows"] + 1), "rows is not the sum"),
            (lambda b: b.update(h=str(int(b["h"]) + 1)), "h is not the sum"),
            (lambda b: b.pop("by_rule"), "missing by_rule"),
            (lambda b: b["by_rule"]["W2_structuring"].update(h="x"), "by_rule[W2_structuring]"),
            (lambda b: b.update(rows=True), "rows or h"),
            (lambda b: b.update(spec=""), "spec"),
        ],
    )
    def test_malformed_is_unavailable(self, mutate, problem):
        body = copy.deepcopy(ASET)
        mutate(body)
        got, _s, why = als.parse_alert_set(_line(body))
        assert got is None and why is not None and problem in why

    def test_bad_json(self):
        got, _s, why = als.parse_alert_set("LB_ALERT_SET {not json}")
        assert got is None and "not valid JSON" in why

    def test_oversized_hash_is_malformed_not_a_crash(self):
        body = copy.deepcopy(ASET)
        body["by_rule"] = {"W2_structuring": {"rows": 8, "h": "9" * 5000}}
        body["rows"], body["h"] = 8, "9" * 5000
        got, _s, why = als.parse_alert_set(_line(body))
        assert got is None and "malformed" in why

    def test_bad_seconds_dropped(self):
        _g, secs, _w = als.parse_alert_set(_line(ASET, seconds=-1))
        assert secs is None

    def test_collector_puts_it_on_the_gold_job(self):
        logs = "[detection] W2_structuring: alerts=5 prior=0 elapsed=1.0s\n" + _line(ASET, 2.5)
        job = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
        assert job.alert_set == ASET
        assert job.alert_set_seconds == 2.5 and job.alert_set_unavailable is None
        assert job.to_dict()["alert_set_seconds"] == 2.5

    def test_spark_side_names_match(self):
        """The tag and spec the scripts print are the ones parsed here."""

        def const(path: Path, name: str):
            for node in ast.parse(path.read_text()).body:
                if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == name for t in node.targets
                ):
                    return ast.literal_eval(node.value)
            raise AssertionError(f"{name} not in {path.name}")

        assert const(SCRIPTS / "common.py", "ALERT_SET_SPEC") == als.ALERT_SET_SPEC
        assert const(SCRIPTS / "gold_finalize_financial.py", "ALERT_SET_TAG") == als.ALERT_SET_TAG
        assert list(const(SCRIPTS / "common.py", "ALERT_SET_COLUMNS")) == ASET["columns"]


# ---------------------------------------------------------------------------
# The record: results.alert_set
# ---------------------------------------------------------------------------


def _fresh(schema="financial", mode="batch", aset=ASET, unavailable=None, system=True):
    """A v1.7 run, with the alert set on gold-finalize. It stamps exp2, or
    exp1 with ``v2_unavailable`` when *system* is False."""
    run = _metrics(_cfg(schema, mode))
    inputs = run.config_snapshot["experiment_inputs"]
    obs, _ = observe(two_nodes(), series_body())
    inputs["corpus_observation"] = obs
    if system:
        inputs["system_identity"] = copy.deepcopy(SYSID)
    gold = run.jobs[-1]
    assert gold.job_type == "gold-finalize"
    gold.alert_set = copy.deepcopy(aset) if aset is not None else None
    gold.alert_set_seconds = 1.0 if aset is not None else None
    gold.alert_set_unavailable = unavailable
    return run


class TestRecord:
    def test_batch_results_carry_it(self):
        e = _fresh().to_dict()["experiment"]
        assert e["schema"] == "exp2"
        assert e["results"]["alert_set"] == ASET
        assert "alert_set_unavailable" not in e["results"]

    def test_unavailable_reason_recorded(self):
        e = _fresh(aset=None, unavailable="boom").to_dict()["experiment"]
        assert "alert_set" not in e["results"]
        assert e["results"]["alert_set_unavailable"] == "boom"
        assert ex.results_established(e) == "the alert-set fingerprint was not recorded (boom)"

    def test_last_gold_job_wins(self):
        """Multi-cycle: gold.alerts holds the last cycle's alerts, so an
        earlier cycle's alert set is never recorded in its place."""
        run = _fresh()
        run.jobs.append(JobMetrics(job_name="g2", job_type="gold-finalize", success=True))
        e = run.to_dict()["experiment"]
        assert "alert_set" not in e["results"]

    def test_continuous_results_do_not_carry_it(self):
        e = _fresh(mode="sustained").to_dict()["experiment"]
        assert "alert_set" not in e["results"]

    def test_c360_needs_none(self):
        e = _fresh(schema="customer360", aset=None).to_dict()["experiment"]
        assert e["schema"] == "exp2"
        assert ex.results_established(e) is True

    def test_benchmark_refresh_keeps_it(self):
        run = _fresh()
        run.experiment = run.to_dict()["experiment"]
        ex.refresh_benchmark(run)
        assert run.experiment["results"]["alert_set"] == ASET


# ---------------------------------------------------------------------------
# Comparison: steps 2, 4 and 5
# ---------------------------------------------------------------------------


class TestCompare:
    @pytest.mark.parametrize(
        "b, expected",
        [
            ({**ASET, "spec": "as2"}, "different definitions"),
            ({**ASET, "cols_sha": "f" * 16}, "different columns"),
            (_aset(W1_connected_components=(3, "-12"), W2_structuring=(6, "907")), "5 alert(s)"),
        ],
    )
    def test_diff_lines(self, b, expected):
        out = als.diff_alert_sets(ASET, b)
        assert len(out) == 1 and expected in out[0]

    def test_diff_ignores_order_of_by_rule(self):
        b = copy.deepcopy(ASET)
        b["by_rule"] = dict(reversed(list(b["by_rule"].items())))
        assert als.diff_alert_sets(ASET, b) == []


# ---------------------------------------------------------------------------
# Release record, report and the continuous guard
# ---------------------------------------------------------------------------


def test_release_record_names_the_rule():
    from lakebench.metrics.release_record import _fingerprint_problems

    other = _aset(W1_connected_components=(2, "-12"), W2_structuring=(5, "907"))
    probs = _fingerprint_problems({"alert_set": other}, {"alert_set": ASET})
    assert probs == [
        "alert set differs: rule W1_connected_components raised 2 alert(s) in the run and 3 "
        "in the expected set (against the expected alert set)"
    ]
    assert _fingerprint_problems({"alert_set": copy.deepcopy(ASET)}, {"alert_set": ASET}) == []
    bad = _fingerprint_problems({"alert_set": "x"}, {"alert_set": ASET})
    assert bad == ["alert set cannot be checked: run not an object, expected well formed"]


def test_release_record_requires_the_alert_set():
    """A 1.7 AML batch release row without its alert set is a problem even
    when the expected entry carries none."""
    from lakebench.metrics.release_record import _results_problems

    exp = _fresh(aset=None, unavailable="boom").to_dict()["experiment"]
    assert _results_problems({}, exp, {"entries": []}) == [
        "the alert-set fingerprint was not recorded (boom)"
    ]
    ok = _fresh().to_dict()["experiment"]
    assert _results_problems({}, ok, {"entries": []}) == [
        "no expected results for this workload, corpus and scale"
    ]


def test_cli_takes_the_fingerprint_off_the_stage_time():
    from datetime import datetime, timezone

    from lakebench.cli._run import _exclude_alert_set_time

    end = datetime(2026, 10, 3, 12, tzinfo=timezone.utc)
    jm = JobMetrics(job_name="g", job_type="gold-finalize", elapsed_seconds=100.0, end_time=end)
    jm.alert_set_seconds = 2.5
    assert _exclude_alert_set_time(jm) == 2.5
    assert jm.elapsed_seconds == 97.5 and (end - jm.end_time).total_seconds() == 2.5
    for secs in (None, 0.0, 100.0):
        jm2 = JobMetrics(job_name="g", job_type="gold-finalize", elapsed_seconds=100.0)
        jm2.alert_set_seconds = secs
        assert _exclude_alert_set_time(jm2) == 0.0 and jm2.elapsed_seconds == 100.0


def test_report_labels_the_fingerprint_seconds():
    from lakebench.reports.generator import ReportGenerator

    run = _fresh()
    run.jobs[-1].elapsed_seconds = 60.0
    run.jobs[-1].alert_set_seconds = 2.5
    html = ReportGenerator.__new__(ReportGenerator)._generate_jobs_table(run)
    assert "excludes 2.5s of Lakebench's alert-set fingerprint" in html


def _names_reachable(root: str) -> dict[str, set[str]]:
    """Names referenced by each script function reachable from *root*.

    Every function defined anywhere in spark/scripts (nested ones too; all
    same-named definitions are followed) is a node; an edge is any
    reference to its name, as a bare name or an attribute (``x.f``), called
    or not, so an alias (``g = common.f``) is followed and caught too."""
    funcs: dict[str, list[ast.AST]] = {}
    for path in sorted(SCRIPTS.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                funcs.setdefault(node.name, []).append(node)
    seen: dict[str, set[str]] = {}
    todo = [root]
    while todo:
        name = todo.pop()
        if name in seen or name not in funcs:
            continue
        refs: set[str] = set()
        for fn in funcs[name]:
            for n in ast.walk(fn):
                if isinstance(n, ast.Name):
                    refs.add(n.id)
                elif isinstance(n, ast.Attribute):
                    refs.add(n.attr)
        seen[name] = refs
        todo += [r for r in refs if r in funcs]
    return seen


def test_no_alert_set_in_tick():
    """S6 by analogy: a continuous tick never fingerprints gold.alerts (a
    Lakebench full scan inside time to detect). Covers run_tick and every
    script function it can reach (gold-finalize's detection driver, the TM
    layer, the rules, common), by any reference, called or aliased."""
    banned = {
        "frame_fingerprint",
        "frame_fingerprint_by",
        "_fingerprint_hash",
        "alert_set_fingerprint",
        "alert_set_line",
    }
    reach = _names_reachable("run_tick")
    assert {"run_tick", "run_detection_rules", "run_tm_operations"} <= set(reach), sorted(reach)
    hits = {f: sorted(c & banned) for f, c in reach.items() if c & banned}
    assert not hits, hits


def test_gold_finalize_prints_the_line_after_detection():
    """The line is printed by main() after detection and the TM layer, last
    in the stage (so the CLI can take its seconds off the stage's end), not
    inside the detection driver (which the tick shares)."""
    tree = ast.parse((SCRIPTS / "gold_finalize_financial.py").read_text())
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    calls = sorted(
        (n.lineno, n.func.id)
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id in ("run_detection_rules", "run_tm_operations", "alert_set_line")
    )
    assert [name for _, name in calls] == [
        "run_detection_rules",
        "run_tm_operations",
        "alert_set_line",
    ]


def test_run_takes_the_fingerprint_off_after_parsing():
    """The cluster path subtracts the alert-set seconds right after it
    applies the parsed driver log, before resources and the journal use
    the stage's time."""
    src = (Path(__file__).resolve().parents[1] / "src/lakebench/cli/_run.py").read_text()
    applied = src.index("_apply_parsed_job_metrics(job_metrics, parsed)")
    excluded = src.index("_exclude_alert_set_time(job_metrics)", applied)
    resources = src.index("_profile = get_job_profile(stage_name, _schema)", applied)
    assert applied < excluded < resources
