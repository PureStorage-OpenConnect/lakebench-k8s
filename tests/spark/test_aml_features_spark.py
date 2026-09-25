"""Executed: aml_features builds the pre-registered per-entity features, and
both adapters (silver tables, bronze corpus) map manifest participants to the
right entity key (AML-GOALS D5, D9, A6)."""

from __future__ import annotations

import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))

# A Monday. Time-zone aware so Spark stores the intended UTC instant whatever
# the host's local zone is (naive datetimes are read as local time).
T0 = datetime(2024, 3, 4, 10, tzinfo=timezone.utc)


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


TXN_SCHEMA = (
    "uetr string, orig_key string, bene_key string, ts timestamp, amount double, "
    "currency string, amount_usd double, orig_country string, bene_country string"
)
ENT_SCHEMA = (
    "key string, is_customer boolean, home_country string, customer_type string, crr_tier string"
)


def _txn(u, o, b, hours, amount=100.0, ccy="USD", oc="US", bc="US"):
    return (u, o, b, T0 + timedelta(hours=hours), amount, ccy, amount, oc, bc)


@pytest.fixture(scope="module")
def feats(spark):
    import aml_features as af

    rows = [
        # A sends to B at 0 h, 24 h, 72 h (gaps 1 d, 2 d), one self-payment.
        _txn("a1", "A", "B", 0, 9_500.0),
        _txn("a2", "A", "B", 24, 200.0),
        _txn("a3", "A", "C", 72, 9_999.0, bc="AE"),
        _txn("a4", "A", "A", 73, 50.0),
        # B sends once, overnight on a Saturday, to a CN resident.
        _txn("b1", "B", "C", 24 * 5 - 8, 1_234.5, bc="CN"),
        # D: four payments inside 24 h, then one a week later.
        _txn("d1", "D", "E", 0),
        _txn("d2", "D", "E", 1),
        _txn("d3", "D", "F", 2),
        _txn("d4", "D", "F", 23),
        _txn("d5", "D", "E", 24 * 7),
        # GBP structuring band: [13,500, 15,000].
        _txn("g1", "G", "H", 0, 14_000.0, ccy="GBP", oc="GB", bc="GB"),
        _txn("g2", "G", "H", 1, 9_600.0, ccy="GBP", oc="GB", bc="GB"),
    ]
    txns = spark.createDataFrame(rows, TXN_SCHEMA)
    ents = spark.createDataFrame(
        [
            ("A", True, "MX", "business", "high"),
            ("B", True, "US", "person", "low"),
            ("C", False, "AE", None, None),
            ("D", True, "US", "person", "medium"),
        ],
        ENT_SCHEMA,
    )
    return {r["key"]: r.asDict() for r in af.entity_features(txns, ents).collect()}


def test_counts_gaps_and_self_payment(feats):
    a = feats["A"]
    # a1..a4 once each (the self-payment a4 is not double counted), plus
    # nothing received.
    assert a["txn_count"] == 4
    assert a["n_sends"] == 4
    # Send gaps: 1 d, 2 d, 1 h.
    gaps = [1.0, 2.0, 1 / 24]
    mean = sum(gaps) / 3
    sd = math.sqrt(sum((g - mean) ** 2 for g in gaps) / 3)
    assert a["gap_mean_days"] == pytest.approx(mean)
    assert a["gap_max_days"] == pytest.approx(2.0)
    assert a["gap_cv"] == pytest.approx(sd / mean)
    assert a["max_gap_over_mean_gap"] == pytest.approx(2.0 / mean)
    assert a["n_counterparties"] == 3  # B, C and itself
    assert a["active_days"] == 3


def test_gap_features_are_sends_only(feats):
    # B sends once and receives twice: one send, so no gap.
    b = feats["B"]
    assert b["txn_count"] == 3 and b["n_sends"] == 1
    assert b["gap_mean_days"] is None and b["gap_cv"] is None
    # C only receives.
    assert feats["C"]["n_sends"] == 0 and feats["C"]["gap_max_days"] is None


def test_burst_24h(feats):
    assert feats["D"]["max_burst_24h"] == 4
    assert feats["E"]["max_burst_24h"] == 2  # d1, d2 inside 24 h; d5 a week later
    assert feats["A"]["max_burst_24h"] == 2  # a1 and a2 are exactly 24 h apart


def test_structuring_band_uses_native_currency(feats):
    # A: 9,500 and 9,999 USD are in [9,000, 10,000]; 200 and 50 are not.
    assert feats["A"]["frac_in_structuring_band"] == pytest.approx(0.5)
    # G: 14,000 GBP is in band, 9,600 GBP is not (USD's band does not apply).
    assert feats["G"]["frac_in_structuring_band"] == pytest.approx(0.5)


def test_entropy_time_and_round(feats):
    # D's five payments fall in hours 10, 11, 12, 9 (next day), 10.
    p = [2 / 5, 1 / 5, 1 / 5, 1 / 5]
    assert feats["D"]["hour_of_day_entropy"] == pytest.approx(-sum(x * math.log2(x) for x in p))
    # G: two payments in two hours, one bit.
    assert feats["G"]["hour_of_day_entropy"] == pytest.approx(math.log2(2))
    # b1 is Saturday 02:00 UTC: overnight and weekend.
    assert feats["B"]["frac_overnight"] == pytest.approx(1 / 3)
    assert feats["B"]["frac_weekend"] == pytest.approx(1 / 3)
    # Round = whole multiple of 100 in native units: 9,500, 200 yes; 9,999, 50 no.
    assert feats["A"]["frac_round_amount"] == pytest.approx(0.5)


def test_country_and_kyc_features(feats):
    a = feats["A"]
    assert a["home_country_high_risk"] == 1.0  # MX is in the corridor pool
    assert feats["B"]["home_country_high_risk"] == 0.0
    assert a["frac_cross_border"] == pytest.approx(0.25)  # only a3 (US -> AE)
    assert a["frac_high_risk_corridor"] == pytest.approx(0.25)
    assert feats["B"]["frac_high_risk_corridor"] == pytest.approx(1 / 3)
    assert a["customer_type"] == 1.0 and a["crr_tier"] == 2.0
    assert feats["B"]["customer_type"] == 0.0 and feats["B"]["crr_tier"] == 0.0
    assert feats["C"]["customer_type"] is None and feats["C"]["is_customer"] is False
    # E has no entity row: not a customer, no attributes.
    assert feats["E"]["is_customer"] is False and feats["E"]["home_country_high_risk"] is None


def test_feature_columns_match_preregistration(feats):
    import json

    import aml_features as af

    prereg = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "src/lakebench/spark/data/aml/aml_preregistration.json"
        ).read_text()
    )
    assert list(af.FEATURE_COLUMNS) + list(af.HISTORY_FEATURE_COLUMNS) == prereg["features"]
    assert set(af.FEATURE_COLUMNS) <= set(feats["A"])


def test_structuring_band_matches_w2(spark):
    """frac_in_structuring_band's band is W2's band (detection_rules)."""
    import aml_features as af
    from detection_rules import _suspicious_amount_expr
    from pyspark.sql.functions import col

    df = spark.createDataFrame(
        [
            (a, c)
            for c in ("USD", "GBP", "JPY", "KRW")
            for a in (100.0, 9_000.0, 9_600.0, 1e4, 1.4e4, 9.5e5, 9.5e6)
        ],
        "txn_amount double, txn_currency string",
    )
    got = df.select(
        af._in_structuring_band(col("txn_amount"), col("txn_currency")).alias("a"),
        _suspicious_amount_expr().alias("b"),
    ).collect()
    assert all(r["a"] == r["b"] for r in got)


# ---------------------------------------------------------------------------
# Adapters and labels
# ---------------------------------------------------------------------------


def _write(spark, rows, schema, path):
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(str(path))
    return str(path)


def _manifest(spark):
    return spark.createDataFrame(
        [
            ("stack", [1, 2, 3], ["u12", "u23"], 11),
            ("dormant_reactivation", [4, 5], ["u45"], 12),
        ],
        "typology_type string, participant_entity_ids array<long>, "
        "participant_uetrs array<string>, seed long",
    )


ACCOUNT_SCHEMA = "holder_entity_id long, iban string"
ACCOUNTS = [(i, f"IB{i}") for i in range(1, 7)] + [(3, "IB3-second")]


def test_bronze_adapter_keys_on_ground_truth_via_iban(spark, tmp_path):
    import aml_features as af

    def party(nm, lei, ctry, iban):
        return (
            {"nm": nm, "pstl_adr": None, "id": {"any_bic": None, "lei": lei}, "ctry_of_res": ctry},
            {"iban": iban},
        )

    pacs_schema = (
        "uetr string, cre_dt_tm timestamp_ntz, intr_bk_sttlm_amt decimal(18,2), "
        "intr_bk_sttlm_ccy string, "
        "dbtr struct<nm:string, pstl_adr:struct<strt_nm:string,twn_nm:string,ctry:string>, "
        "id:struct<any_bic:string,lei:string>, ctry_of_res:string>, dbtr_acct struct<iban:string>, "
        "cdtr struct<nm:string, pstl_adr:struct<strt_nm:string,twn_nm:string,ctry:string>, "
        "id:struct<any_bic:string,lei:string>, ctry_of_res:string>, cdtr_acct struct<iban:string>"
    )
    from decimal import Decimal

    lei = {i: f"LEI{i:017d}" for i in range(1, 7)}
    # Entities 5 and 6 share an LEI: silver would merge them, bronze must not
    # (A6 exists to catch exactly that difference).
    lei[6] = lei[5]

    def row(u, o, b, h):
        do, da = party(f"n{o}", lei[o], "US", f"IB{o}")
        bo, ba = party(f"n{b}", lei[b], "US", f"IB{b}")
        return (
            u,
            T0.replace(tzinfo=None) + timedelta(hours=h),
            Decimal("100.00"),
            "USD",
            do,
            da,
            bo,
            ba,
        )

    rows = [row("u12", 1, 2, 0), row("u23", 2, 3, 5), row("u45", 4, 5, 9), row("u56", 5, 6, 12)]
    # A payment whose creditor IBAN is not in the account master.
    stray = list(row("u99", 1, 2, 20))
    stray[7] = {"iban": "IB-unknown"}
    rows.append(tuple(stray))
    pacs = _write(spark, rows, pacs_schema, tmp_path / "pacs")
    party_path = _write(
        spark,
        [
            (i, "US", i != 3, "person" if i != 3 else None, "low" if i != 3 else None)
            for i in range(1, 7)
        ],
        "entity_id long, country string, is_customer boolean, customer_type string, crr_tier string",
        tmp_path / "party",
    )
    acct = _write(spark, ACCOUNTS, ACCOUNT_SCHEMA, tmp_path / "acct")

    txns, ents, id_map = af.bronze_frames(
        spark, pacs_path=pacs, party_path=party_path, account_path=acct
    )
    assert af.duplicate_ibans(spark, acct) == 0
    dup = _write(spark, ACCOUNTS + [(4, "IB5")], ACCOUNT_SCHEMA, tmp_path / "acct-dup")
    assert af.duplicate_ibans(spark, dup) == 1
    assert dict(id_map.select("dg_id", "key").collect()) == {i: i for i in range(1, 7)}
    keys = {r["orig_key"] for r in txns.collect()} | {r["bene_key"] for r in txns.collect()}
    assert keys == {1, 2, 3, 4, 5, 6, None}
    assert af.unkeyed_rows(txns) == 1
    from pyspark.sql.functions import hour

    # TIMESTAMP_NTZ wall clock 10:00 becomes the 10:00 UTC instant.
    assert dict(txns.select("uetr", hour("ts")).collect())["u12"] == 10
    labels = {
        (r["key"], r["typology_type"])
        for r in af.labels_from_participants(_manifest(spark), id_map).collect()
    }
    assert labels == {
        (1, "stack"),
        (2, "stack"),
        (3, "stack"),
        (4, "dormant_reactivation"),
        (5, "dormant_reactivation"),
    }
    # Subject role: stack's first pass-through (index 1), dormant's originator.
    subj = {
        (r["key"], r["typology_type"])
        for r in af.labels_from_subjects(spark, _manifest(spark), id_map).collect()
    }
    assert subj == {(2, "stack"), (4, "dormant_reactivation")}
    # The UETR route agrees for these instances (every participant is on a row).
    by_uetr = af.labels_from_uetrs(_manifest(spark), txns)
    agree = af.label_agreement(af.labels_from_participants(_manifest(spark), id_map), by_uetr)
    assert agree["stack"] == {"by_participant_id": 3, "by_uetr": 3, "both": 3}

    feats = af.entity_features(txns, ents)
    pdf = af.gate_frame(
        feats,
        af.labels_from_participants(_manifest(spark), id_map),
        ["stack", "dormant_reactivation"],
    )
    # Customers only: entity 3 (non-customer participant) is not scored.
    assert len(pdf) == 5
    assert pdf["label:stack"].sum() == 2 and pdf["label:dormant_reactivation"].sum() == 2
    assert "key" not in pdf.columns and sorted(pdf["group"]) == [1, 2, 4, 5, 6]


def test_silver_adapter_maps_participants_to_silver_ids(spark, tmp_path):
    import aml_features as af

    spark.sql("CREATE DATABASE IF NOT EXISTS lbsilver")
    sid = {i: 1000 + i for i in range(1, 7)}
    txn_rows = [
        ("u12", sid[1], sid[2], T0, 100.0, "USD", 100.0),
        ("u23", sid[2], sid[3], T0 + timedelta(hours=5), 14_000.0, "GBP", 18_200.0),
        ("u45", sid[4], sid[5], T0 + timedelta(hours=9), 50.0, "USD", 50.0),
    ]
    tables = {
        "t": (
            txn_rows,
            "uetr string, originator_id long, beneficiary_id long, txn_timestamp timestamp, "
            "txn_amount double, txn_currency string, txn_amount_usd double",
        ),
        "e": (
            [(sid[i], "US" if i != 2 else "CN", i != 3, "person", "low") for i in range(1, 7)],
            "entity_id long, country string, is_customer boolean, customer_type string, crr_tier string",
        ),
        "a": (
            [(f"IB{i}", sid[i]) for i in range(1, 7)],
            "iban string, holder_entity_id long",
        ),
    }
    for name, (rows, schema) in tables.items():
        spark.sql(f"DROP TABLE IF EXISTS lbsilver.{name}")
        spark.createDataFrame(rows, schema).write.option("path", str(tmp_path / name)).saveAsTable(
            f"lbsilver.{name}"
        )
    acct = _write(spark, ACCOUNTS, ACCOUNT_SCHEMA, tmp_path / "acct")
    txns, ents, id_map = af.silver_frames(
        spark,
        catalog="spark_catalog",
        txns_table="lbsilver.t",
        entities_table="lbsilver.e",
        accounts_table="lbsilver.a",
        account_path=acct,
    )
    assert dict(id_map.collect()) == sid
    t = {r["uetr"]: r for r in txns.collect()}
    assert t["u12"]["bene_country"] == "CN" and t["u12"]["orig_country"] == "US"
    labels = {
        (r["key"], r["typology_type"])
        for r in af.labels_from_participants(_manifest(spark), id_map).collect()
    }
    assert (sid[3], "stack") in labels and (sid[5], "dormant_reactivation") in labels
    f = {r["key"]: r for r in af.entity_features(txns, ents).collect()}
    assert f[sid[2]]["frac_in_structuring_band"] == pytest.approx(0.5)  # 14,000 GBP
    assert f[sid[1]]["frac_high_risk_corridor"] == pytest.approx(1.0)  # counterparty in CN


def test_density_and_timing_counts(spark):
    import aml_features as af

    rows = [_txn(f"x{i}", "P", "Q", 24 * i) for i in range(30)] + [_txn("u12", "R", "S", 0)]
    txns = spark.createDataFrame(rows, TXN_SCHEMA)
    d = af.typology_density(txns, _manifest(spark))
    assert d == {"total_rows": 31, "planted_rows": 1, "per_typology": {"stack": 1}}
    ents = spark.createDataFrame([("P", True, "US", "person", "low")], ENT_SCHEMA)
    feats = af.entity_features(txns, ents)
    # P sends daily: gap_cv 0, in the cohort at 20 sends.
    c = af.timing_mixture_counts(feats, cohort_min_sends=20, low_cv_edge=0.5, high_cv_edge=1.0)
    assert c == {"n_cohort": 1, "n_below_low": 1, "n_above_high": 0}


def test_subject_index_mirrors_generator():
    import aml_features as af

    # Standard splitmix64 first output for state 0.
    assert af._splitmix64(0) == 0xE220A8397B1DCDAF
    assert af.subject_index("micro_structuring", 9, 5) == 8
    assert af.subject_index("fan_in", 6, 5) == 5
    assert af.subject_index("rapid_layering", 3, 5) == 1
    assert af.subject_index("gather_scatter", 9, 5) == 0
    flips = {af.subject_index("corridor_high_risk", 2, s) for s in range(-50, 50)}
    assert flips == {0, 1}
    # Negative i64 seeds are read as their u64 bit pattern, as in Rust.
    assert af.subject_index("corridor_high_risk", 2, -1) == af._splitmix64((1 << 64) - 1) & 1


def test_manifest_glob_reads_every_cycle_and_nothing_else(spark, tmp_path):
    import aml_features as af

    d = tmp_path / "manifest"
    schema = "typology_id string, typology_type string, seed long"
    for name, tid in (
        ("manifest.parquet", "STACK_7_0000000"),
        ("manifest-c001.parquet", "STACK_7_0000001"),
        ("manifest-backup.parquet", "STACK_7_0000002"),
        ("manifest-c1000.parquet", "STACK_7_0000003"),
    ):
        spark.createDataFrame([(tid, "stack", 1)], schema).write.parquet(str(d / name))
    want = ["STACK_7_0000000", "STACK_7_0000001", "STACK_7_0000003"]
    # Same rows whether the caller passes cycle 0's path or the cluster's glob.
    for uri in (str(d / "manifest.parquet"), str(d / "manifest*.parquet")):
        got = af.read_manifest(spark, uri)
        assert sorted(r["typology_id"] for r in got.collect()) == want
        assert got.columns == ["typology_id", "typology_type", "seed"]
    af.check_manifest(got)
    with pytest.raises(ValueError, match="repeat typology_id"):
        af.check_manifest(got.unionByName(got))
    assert af.manifest_glob("/x/other.parquet") == "/x/other.parquet"


def test_corpus_seed_check(spark):
    import aml_features as af

    def iseed(seed, tid, j):
        inner = af._splitmix64(0xF100 + tid * 100_000_000 + j)
        v = af._splitmix64((seed & ((1 << 64) - 1)) ^ inner)
        return v - (1 << 64) if v >= 1 << 63 else v  # stored as i64

    rows = [(f"DORMANT_REACTIVATION_11_{j:07d}", iseed(42, 11, j)) for j in range(5)]
    m = spark.createDataFrame(
        rows + [(None, 5), ("X_1_0000000", None)], "typology_id string, seed long"
    )
    assert af.corpus_seed_check(m, 42)["matched_share"] == 1.0
    assert af.corpus_seed_check(m, 50000042)["matched_share"] == 0.0
    assert af.corpus_seed_check(m, None)["matched_share"] is None


def test_subject_labels_refuse_a_missing_seed(spark):
    import aml_features as af

    m = spark.createDataFrame(
        [("stack", [1, 2, 3], None)],
        "typology_type string, participant_entity_ids array<long>, seed long",
    )
    ids = spark.createDataFrame([(1, 1)], "dg_id long, key long")
    with pytest.raises(ValueError, match="instance seed"):
        af.labels_from_subjects(spark, m, ids)
