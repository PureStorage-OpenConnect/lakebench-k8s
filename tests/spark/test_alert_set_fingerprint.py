"""EVD-10 (ER-15): the alert-set fingerprint over (rule, subject, window).

``common.alert_set_fingerprint`` on a fixed 50-row frame with every
gold.alerts column gives one pinned value on the 4.0 and 4.1 lines (REL-2's
cross-row AML equality depends on it); row order does not matter; dropping an
alert or changing one subject changes it, and only for that rule; generated
ids and wall-clock times do not. ``gold_finalize_financial.alert_set_line``
prints it in the form the collector parses.

No jars: the helper reads a DataFrame or a temp view, not a table.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script_module")

T0 = datetime(2024, 5, 6, 8, tzinfo=timezone.utc)
RULES = ("W1_connected_components", "W2_structuring", "W4_risk_propagation", "W5_sanctions")
RUN = "run-a"

# The committed alert set of _rows() (rows, h, cols_sha and per-rule rows and
# h). Asserted on both CI Spark legs (pyspark 4.0.1 and 4.1.1); recompute only
# when common._FINGERPRINT_VERSION or ALERT_SET_SPEC changes.
PINNED = {
    "rows": 50,
    "h": "-13338934748839768034",
    "cols_sha": "24c1812ea031d0d3",
    "by_rule": {
        "W1_connected_components": {"rows": 13, "h": "7147854260006584348"},
        "W2_structuring": {"rows": 13, "h": "447616481268379403"},
        "W4_risk_propagation": {"rows": 12, "h": "-217201869969663578"},
        "W5_sanctions": {"rows": 12, "h": "-20717203620145068207"},
    },
}


def _schema():
    from detection_rules import ALERT_COLUMNS

    return ", ".join(f"{name} {dtype}" for name, dtype, _null in ALERT_COLUMNS)


def _rows(run_id=RUN, detected=None):
    rows = []
    for i in range(50):
        rows.append(
            (
                f"00000000-0000-0000-0000-{i:012d}",  # alert_id
                RULES[i % len(RULES)],
                "1",
                "m",
                "1",
                10_000 + (i * 37) % 23,  # entity_id: repeats across rules
                None if i % 6 == 0 else [f"U{i}", f"U{i + 1}"],
                None if i % 5 == 0 else [i, i + 1],
                T0 + timedelta(minutes=13 * i, microseconds=i),  # alert_ts
                i * 0.5,
                ("HIGH", "MEDIUM", "LOW")[i % 3],
                "open",
                None,
                "rule",
                run_id,
                f"alert {i}",
                None if i % 4 == 0 else {"k": str(i)},
                detected or (T0 + timedelta(days=30, seconds=i)),
                ["R1"] if i % 2 else None,
            )
        )
    return rows


def _df(spark, rows):
    return spark.createDataFrame(rows, _schema())


def _aset(df):
    from common import alert_set_fingerprint

    return alert_set_fingerprint(df)


def _with(rows, i, **changes):
    from detection_rules import ALERT_COLUMNS

    names = [c[0] for c in ALERT_COLUMNS]
    row = list(rows[i])
    for name, value in changes.items():
        row[names.index(name)] = value
    out = list(rows)
    out[i] = tuple(row)
    return out


@pytest.fixture(scope="module")
def base(load_script_module, spark_session):
    return _df(spark_session, _rows())


def test_pinned_cross_line_value(base):
    got = _aset(base)
    assert got["spec"] == "as1" and got["columns"] == ["rule_id", "entity_id", "alert_ts"]
    assert {k: got[k] for k in PINNED} == PINNED


def test_totals_equal_the_whole_frame_fingerprint(base):
    """The per-rule sums are frame_fingerprint of the whole frame, and each
    rule's entry is frame_fingerprint of that rule's rows."""
    from common import frame_fingerprint
    from pyspark.sql import functions as F

    got = _aset(base)
    cols = ["rule_id", "entity_id", "alert_ts"]
    assert (got["rows"], got["h"], got["cols_sha"]) == frame_fingerprint(base, cols)
    for rule, part in got["by_rule"].items():
        n, h, _ = frame_fingerprint(base.where(F.col("rule_id") == rule), cols)
        assert part == {"rows": n, "h": h}, rule


def test_shuffled_rows_equal(spark_session, base):
    rows = _rows()
    random.Random(43).shuffle(rows)
    assert _aset(_df(spark_session, rows).repartition(3)) == _aset(base)


def test_removed_alert_differs_for_its_rule_only(spark_session, base):
    rows = _rows()
    dropped = rows[:7] + rows[8:]  # row 7 is RULES[3]
    a, b = _aset(_df(spark_session, dropped)), _aset(base)
    rule = RULES[7 % len(RULES)]
    assert a["rows"] == b["rows"] - 1 and a["h"] != b["h"]
    assert a["by_rule"][rule]["rows"] == b["by_rule"][rule]["rows"] - 1
    assert {r: v for r, v in a["by_rule"].items() if r != rule} == {
        r: v for r, v in b["by_rule"].items() if r != rule
    }


def test_duplicated_alert_differs(spark_session, base):
    rows = _rows()
    a = _aset(_df(spark_session, rows + [rows[3]]))
    assert a["rows"] == 51 and a["h"] != _aset(base)["h"]


def test_changed_subject_differs(spark_session, base):
    rows = _rows()
    a = _aset(_df(spark_session, _with(rows, 10, entity_id=rows[10][5] + 1)))
    b = _aset(base)
    rule = RULES[10 % len(RULES)]
    assert a["by_rule"][rule]["h"] != b["by_rule"][rule]["h"]
    assert a["by_rule"][rule]["rows"] == b["by_rule"][rule]["rows"]


def test_changed_window_differs(spark_session, base):
    rows = _rows()
    a = _aset(_df(spark_session, _with(rows, 11, alert_ts=rows[11][8] + timedelta(microseconds=1))))
    assert a["h"] != _aset(base)["h"]


def test_generated_ids_and_wall_clock_do_not_count(spark_session, base):
    """alert_id, run_id, detected_ts (and every other column outside the
    tuple) can differ between two runs that raised the same alerts."""
    rows = [
        _with([r], 0, alert_id=f"other-{i}", alert_score=99.0, narrative="x")[0]
        for i, r in enumerate(_rows(run_id="run-b", detected=T0 + timedelta(days=400)))
    ]
    assert _aset(_df(spark_session, rows)) == _aset(base)


def test_empty_frame(spark_session):
    got = _aset(_df(spark_session, []))
    assert (got["rows"], got["h"], got["by_rule"]) == (0, "0", {})


def test_gold_finalize_line_round_trips(spark_session, load_script_module):
    """alert_set_line over a view of the table, scoped to the run, parsed by
    the collector: the same alert set, and the seconds it took."""
    from lakebench.metrics.alert_set import parse_alert_set

    gf = load_script_module("gold_finalize_financial")
    mixed = _rows() + _rows(run_id="older-run")[:5]
    _df(spark_session, mixed).createOrReplaceTempView("er15_alerts")
    line = gf.alert_set_line(spark_session, RUN, table="er15_alerts")
    assert line.startswith("LB_ALERT_SET {")
    got, seconds, why = parse_alert_set("[lb] 2026-10-03T00:00:00 - " + line)
    assert why is None and got == _aset(_df(spark_session, _rows()))
    assert seconds is not None and seconds >= 0


def test_gold_finalize_line_unavailable(spark_session, load_script_module):
    from lakebench.metrics.alert_set import parse_alert_set

    gf = load_script_module("gold_finalize_financial")
    line = gf.alert_set_line(spark_session, RUN, table="er15_no_such_table")
    got, _s, why = parse_alert_set(line)
    assert got is None and why and "er15_no_such_table" in why
