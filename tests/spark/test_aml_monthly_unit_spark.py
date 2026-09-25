"""Executed: the DRAFT (customer, UTC month) unit -- window membership,
history features, and subject-only monthly labels with exclusions."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))

CFG = {"lead_in_days": 14, "burn_in_months": 1, "history_days": 60}


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def t(m, d, h=12):
    return datetime(2024, m, d, h, tzinfo=timezone.utc)


TXN_SCHEMA = (
    "uetr string, orig_key long, bene_key long, ts timestamp, amount double, "
    "currency string, amount_usd double, orig_country string, bene_country string"
)
ROWS = [
    ("u0", 2, 3, t(1, 5)),  # corpus starts 2024-01
    ("u10", 1, 2, t(2, 15)),
    ("u1", 5, 1, t(3, 10)),  # stack planted, month 2
    ("u2", 1, 6, t(4, 2)),  # stack planted, month 3 (completion)
    ("u11", 1, 2, t(5, 20)),
    ("u3", 7, 8, t(2, 1)),  # dormant anchor (before the burst start)
    ("u4", 7, 8, t(6, 5)),  # dormant burst, month 5
]


@pytest.fixture(scope="module")
def frames(spark):
    import aml_features as af

    txns = spark.createDataFrame(
        [(u, o, b, ts, 100.0, "USD", 100.0, "US", "US") for u, o, b, ts in ROWS], TXN_SCHEMA
    )
    ents = spark.createDataFrame(
        [(k, True, "US", "person", "low") for k in range(1, 9)],
        "key long, is_customer boolean, home_country string, customer_type string, crr_tier string",
    )
    windows = af.MonthWindows.from_txns(txns, CFG)
    return txns, ents, windows


def test_window_membership(spark, frames):
    from pyspark.sql.functions import col

    _, _, w = frames
    assert (w.y0, w.m0, w.n_months) == (2024, 1, 6)
    df = spark.createDataFrame([(t(2, 20),), (t(2, 10),)], "ts timestamp")
    a = {(r["ts"].day, r["m"]) for r in df.select("ts", w.a_months(col("ts")).alias("m")).collect()}
    # Feb 20 + 14 d is in March, so the row is in A(Feb) and A(Mar).
    assert a == {(20, 1), (20, 2), (10, 1)}
    h = sorted(
        r["m"]
        for r in df.filter(col("ts") == t(2, 20)).select(w.h_months(col("ts")).alias("m")).collect()
    )
    # H(m) = [start(m) - 74 d, start(m) - 14 d): Feb 20 is in H(Apr) and H(May).
    assert h == [3, 4]


def test_windowed_features(frames):
    import aml_features as af

    txns, ents, w = frames
    f = {(r["key"], r["month"]): r.asDict() for r in af.entity_features(txns, ents, w).collect()}
    assert not any(m == 0 for _, m in f)  # burn-in month is history only
    apr = f[(1, 3)]
    assert apr["txn_count"] == 1 and apr["n_sends"] == 1
    assert apr["days_since_prior_send"] == pytest.approx(47.0)  # Feb 15 -> Apr 2
    assert apr["txn_count_vs_history"] == pytest.approx(1 / (2 / (60 / (365.25 / 12))))
    assert apr["amount_mean_vs_history_median"] == pytest.approx(1.0)
    assert apr["frac_counterparties_new"] == pytest.approx(1.0)
    feb = f[(1, 1)]
    assert feb["txn_count_vs_history"] is None and feb["days_since_prior_send"] is None
    mar = f[(1, 2)]
    assert mar["n_sends"] == 0 and mar["days_since_prior_send"] is None
    may = f[(1, 4)]
    assert may["days_since_prior_send"] == pytest.approx(48.0)  # Apr 2 -> May 20
    # A(May) holds only u11 to entity 2; the history (Feb 17 - Apr 17) saw 5 and 6.
    assert may["frac_counterparties_new"] == pytest.approx(1.0)
    assert set(af.FEATURE_COLUMNS) | set(af.HISTORY_FEATURE_COLUMNS) <= set(apr)


def _manifest(spark):
    return spark.createDataFrame(
        [
            ("STACK_7_0000000", "stack", [5, 1, 6], ["u1", "u2"], 1, t(3, 1)),
            (
                "DORMANT_REACTIVATION_11_0000000",
                "dormant_reactivation",
                [7, 8],
                ["u3", "u4"],
                2,
                t(6, 1),
            ),
        ],
        "typology_id string, typology_type string, participant_entity_ids array<long>, "
        "participant_uetrs array<string>, seed long, injection_ts_start timestamp",
    )


def test_monthly_labels_and_frame(spark, frames):
    import aml_features as af

    txns, ents, w = frames
    id_map = spark.createDataFrame([(k, k) for k in range(1, 9)], "dg_id long, key long")
    ts = ["stack", "dormant_reactivation"]
    labels, counts = af.monthly_labels(spark, _manifest(spark), id_map, txns, w, ts)
    got = {(r["key"], r["month"], r["typology_type"], r["kind"]) for r in labels.collect()}
    assert got == {
        (1, 3, "stack", "positive"),
        (1, 2, "stack", "excluded_incomplete"),
        (5, 2, "stack", "excluded_nonsubject"),
        (5, 3, "stack", "excluded_nonsubject"),
        (6, 3, "stack", "excluded_nonsubject"),
        # The anchor (Feb) does not set the completion month, but its month is
        # still excluded: it holds a planted row.
        (7, 5, "dormant_reactivation", "positive"),
        (7, 1, "dormant_reactivation", "excluded_incomplete"),
        (8, 5, "dormant_reactivation", "excluded_nonsubject"),
        (8, 1, "dormant_reactivation", "excluded_nonsubject"),
    }
    assert counts["per_typology"]["stack"]["instances"] == 1
    assert counts["per_typology"]["stack"]["units_positive"] == 1

    feats = af.entity_features(txns, ents, w)
    pdf = af.gate_frame_monthly(feats, labels, ts).set_index(["group", "month"])
    assert pdf.loc[(1, 3), "label:stack"] == 1 and pdf.loc[(1, 3), "exclude:stack"] == 0
    assert pdf.loc[(1, 2), "label:stack"] == 0 and pdf.loc[(1, 2), "exclude:stack"] == 1
    assert pdf.loc[(1, 4), "label:stack"] == 0 and pdf.loc[(1, 4), "exclude:stack"] == 0
    assert pdf.loc[(7, 5), "label:dormant_reactivation"] == 1


def test_build_gate_inputs_monthly(spark, frames):
    import aml_features as af

    txns, ents, _ = frames
    id_map = spark.createDataFrame([(k, k) for k in range(1, 9)], "dg_id long, key long")
    prereg = {"unit_of_scoring": {"window": "utc_calendar_month", **CFG}}
    ts = ["stack", "dormant_reactivation"]
    out = af.build_gate_inputs(
        spark,
        prereg,
        txns=txns,
        ents=ents,
        id_map=id_map,
        manifest=_manifest(spark),
        role="subject",
        typologies=ts,
    )
    assert out["unit"]["window"] == "utc_calendar_month"
    assert out["unit"]["first_scored_month"] == 1 and out["unit"]["n_units"] == len(out["primary"])
    assert "secondary_lifetime" in out and len(out["secondary_lifetime"]) == 7  # 4 never pays
    with pytest.raises(ValueError, match="subject-role"):
        af.build_gate_inputs(
            spark,
            prereg,
            txns=txns,
            ents=ents,
            id_map=id_map,
            manifest=_manifest(spark),
            role="participant",
            typologies=ts,
        )
