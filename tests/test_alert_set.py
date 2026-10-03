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
from lakebench.metrics import comparability as cmp
from lakebench.metrics import experiment as ex
from lakebench.metrics.collector import JobMetrics, MetricsCollector
from tests.fixtures import stored_records as sr
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


def _fresh(schema="financial", mode="batch", aset=ASET, unavailable=None):
    """A v1.7 run that stamps exp2, with the alert set on gold-finalize."""
    run = _metrics(_cfg(schema, mode))
    inputs = run.config_snapshot["experiment_inputs"]
    obs, _ = observe(two_nodes(), series_body())
    inputs["corpus_observation"] = obs
    inputs["system_identity"] = copy.deepcopy(SYSID)
    gold = run.jobs[-1]
    assert gold.job_type == "gold-finalize"
    gold.alert_set = copy.deepcopy(aset) if aset is not None else None
    gold.alert_set_seconds = 1.0 if aset is not None else None
    gold.alert_set_unavailable = unavailable
    return run


def _record(run, run_id):
    d = run.to_dict()
    d["run_id"] = run_id
    return d


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


def _with_aset(run_id: str, new_id: str, aset):
    rec = sr.load_record(run_id)
    rec["run_id"] = new_id
    if aset is not None:
        rec["experiment"]["results"]["alert_set"] = copy.deepcopy(aset)
    return rec


class TestCompare:
    def test_alert_set_refusal(self):
        """Two copies of 1320bd (AML batch, exp1) with one rule's hash
        changed read NOT COMPARABLE, different results, naming the rule."""
        changed = copy.deepcopy(ASET)
        changed["by_rule"]["W2_structuring"]["h"] = "908"
        changed["h"] = str(int(changed["h"]) + 1)
        a = _with_aset("1320bd", "a", ASET)
        b = _with_aset("1320bd", "b", changed)
        v = cmp.pair_verdict([a], [b])
        assert (v.verdict, v.step, v.code) == (cmp.NOT_COMPARABLE, "5", 10)
        assert v.reasons == [
            "alert set differs: rule W2_structuring raised 5 alert(s) in A and 5 in B "
            "(same count, different alerts)"
        ]
        # The legacy refusals path agrees.
        _prov, results, _notes = ex.refusals(a, b)
        assert results == v.reasons

    def test_equal_alert_sets_are_a_repeat(self):
        v = cmp.pair_verdict([_with_aset("1320bd", "a", ASET)], [_with_aset("1320bd", "b", ASET)])
        assert (v.verdict, v.attribution) == (cmp.LIKE_FOR_LIKE, "repeat")

    def test_inside_one_side_is_step_2(self):
        other = _aset(W1_connected_components=(3, "-12"))
        v = cmp.pair_verdict(
            [_with_aset("1320bd", "a1", ASET), _with_aset("1320bd", "a2", other)],
            [_with_aset("1320bd", "b", ASET)],
        )
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "2")
        assert any(
            "rule W2_structuring raised 5 alert(s) in a1 and none in a2" in r for r in v.reasons
        )

    def test_exp1_absent_on_one_side_is_a_note(self):
        """d1: a 1.6 record never had an alert set; absent on one exp1 side
        is a note and the pair reads on its benchmark results."""
        v = cmp.pair_verdict([_with_aset("1320bd", "a", ASET)], [_with_aset("1320bd", "b", None)])
        assert v.verdict == cmp.LIKE_FOR_LIKE
        assert any("recorded no alert-set fingerprint" in n for n in v.notes)

    def test_exp1_absent_on_both_sides_unchanged(self):
        """Stored 1.6 AML pairs keep their verdicts and notes."""
        base = cmp.pair_verdict([sr.load_record("1320bd")], [_with_aset("1320bd", "b", None)])
        assert base.verdict == cmp.LIKE_FOR_LIKE
        assert not any("alert-set" in n for n in base.notes)

    def test_exp2_missing_alert_set_not_established(self):
        """d2-5: an exp2 AML batch record without an alert set is NOT
        ESTABLISHED, never let through on its benchmark results."""
        a, b = _record(_fresh(), "a"), _record(_fresh(aset=None), "b")
        v = cmp.pair_verdict([a], [b])
        assert (v.verdict, v.step, v.code) == (cmp.NOT_ESTABLISHED, "4", 11)
        assert v.reasons == ["B run b: the alert-set fingerprint was not recorded"]
        # The same two blocks read as exp1 (the d1 note rule) are like-for-like.
        for rec in (a, b):
            rec["experiment"]["schema"] = "exp1"
            rec["experiment"].pop("identity_version", None)
        assert ex.results_established(b["experiment"]) is True

    def test_exp2_both_present_equal_and_different(self):
        a, b = _record(_fresh(), "a"), _record(_fresh(), "b")
        assert cmp.pair_verdict([a], [b]).verdict == cmp.LIKE_FOR_LIKE
        b["experiment"]["results"]["alert_set"] = _aset(W1_connected_components=(3, "-12"))
        v = cmp.pair_verdict([a], [b])
        assert (v.verdict, v.step) == (cmp.NOT_COMPARABLE, "5")

    def test_exp2_malformed_alert_set_not_established(self):
        a, b = _record(_fresh(), "a"), _record(_fresh(), "b")
        b["experiment"]["results"]["alert_set"]["rows"] = 99
        v = cmp.pair_verdict([a], [b])
        assert (v.verdict, v.step) == (cmp.NOT_ESTABLISHED, "4")
        assert "malformed" in v.reasons[0]

    def test_refusals_note_when_missing_on_exp2(self):
        a, b = _record(_fresh(), "a"), _record(_fresh(aset=None), "b")
        _prov, results, notes = ex.refusals(a, b)
        assert results == []
        assert notes == [
            "result equivalence not checked: B: the alert-set fingerprint was not recorded"
        ]

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


def test_report_labels_the_fingerprint_seconds():
    from lakebench.reports.generator import ReportGenerator

    run = _fresh()
    run.jobs[-1].elapsed_seconds = 60.0
    run.jobs[-1].alert_set_seconds = 2.5
    html = ReportGenerator.__new__(ReportGenerator)._generate_jobs_table(run)
    assert "includes 2.5s of Lakebench's alert-set fingerprint" in html


def _calls_reachable(roots: list[tuple[Path, str]]) -> dict[str, set[str]]:
    """Names called by each function reachable from *roots*, following calls
    to functions defined in the same scripts (by bare or attribute name)."""
    funcs: dict[str, ast.AST] = {}
    for path in {p for p, _ in roots} | {SCRIPTS / "gold_finalize_financial.py"}:
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.FunctionDef):
                funcs.setdefault(node.name, node)
    seen: dict[str, set[str]] = {}
    todo = [name for _, name in roots]
    while todo:
        name = todo.pop()
        if name in seen or name not in funcs:
            continue
        called = set()
        for n in ast.walk(funcs[name]):
            if isinstance(n, ast.Call):
                f = n.func
                called.add(f.id if isinstance(f, ast.Name) else getattr(f, "attr", ""))
        seen[name] = called
        todo += [c for c in called if c in funcs]
    return seen


def test_no_alert_set_in_tick():
    """S6 by analogy: a continuous tick never fingerprints gold.alerts (a
    Lakebench full scan inside time to detect). Covers run_tick and every
    script function it reaches, including gold-finalize's detection driver."""
    banned = {
        "frame_fingerprint",
        "frame_fingerprint_by",
        "alert_set_fingerprint",
        "alert_set_line",
    }
    reach = _calls_reachable([(SCRIPTS / "gold_refresh_financial.py", "run_tick")])
    assert "run_tick" in reach and "run_detection_rules" in reach, sorted(reach)
    hits = {f: sorted(c & banned) for f, c in reach.items() if c & banned}
    assert not hits, hits


def test_gold_finalize_prints_the_line_after_detection():
    """The line is printed by main() after run_detection_rules, not inside
    the detection driver (which the tick shares)."""
    tree = ast.parse((SCRIPTS / "gold_finalize_financial.py").read_text())
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    calls = sorted(
        (n.lineno, n.func.id)
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id in ("run_detection_rules", "alert_set_line")
    )
    assert [name for _, name in calls] == ["run_detection_rules", "alert_set_line"]
