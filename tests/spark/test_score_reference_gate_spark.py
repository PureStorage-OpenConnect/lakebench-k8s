"""Executed: score_financial_reference.py runs the fidelity gate over silver
tables end to end (silver adapter, labels, gate, report and metrics writers),
the cluster half of AML-GOALS D9."""

from __future__ import annotations

import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("sklearn")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/lakebench/spark/scripts"))
sys.path.insert(0, str(ROOT / "src/lakebench/aml"))


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


def _silver(spark, tmp_path):
    rnd = random.Random(1)
    n = 240
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    txns = []
    for i in range(3000):
        o, b = rnd.randrange(n), rnd.randrange(n)
        txns.append(
            (
                f"u{i}",
                o,
                b,
                t0 + timedelta(hours=rnd.randrange(24 * 365)),
                100.0 + i,
                "USD",
                100.0 + i,
            )
        )
    ents = [(i, "US" if i % 7 else "CN", i % 2 == 0, "person", "low") for i in range(n)]
    accts = [(f"IB{i}", i) for i in range(n)]
    spark.sql("CREATE DATABASE IF NOT EXISTS refsilver")
    for name, rows, schema in (
        (
            "transactions",
            txns,
            "uetr string, originator_id long, beneficiary_id long, txn_timestamp timestamp, "
            "txn_amount double, txn_currency string, txn_amount_usd double",
        ),
        (
            "entities",
            ents,
            "entity_id long, country string, is_customer boolean, customer_type string, crr_tier string",
        ),
        ("accounts", accts, "iban string, holder_entity_id long"),
    ):
        spark.sql(f"DROP TABLE IF EXISTS refsilver.{name}")
        spark.createDataFrame(rows, schema).write.option("path", str(tmp_path / name)).saveAsTable(
            f"refsilver.{name}"
        )
    acct = str(tmp_path / "account.parquet")
    # Datagen ids are silver id + 10,000; the IBAN is the only link.
    spark.createDataFrame(
        [(i + 10_000, f"IB{i}") for i in range(n)], "holder_entity_id long, iban string"
    ).write.parquet(acct)
    types = [
        "gather_scatter",
        "rapid_layering",
        "stack",
        "dormant_reactivation",
        "micro_structuring",
        "corridor_high_risk",
    ]
    man = [
        (
            t,
            [10_000 + ((k * 13 + j) % n) for j in range(24)],
            [f"u{k * 30 + j}" for j in range(3)],
            "datagen-v2-rs-0.2",
        )
        for k, t in enumerate(types)
    ]
    manifest = spark.createDataFrame(
        man,
        "typology_type string, participant_entity_ids array<long>, "
        "participant_uetrs array<string>, model_version string",
    )
    return manifest, acct


def test_fidelity_gate_over_silver(spark, tmp_path, monkeypatch):
    import score_financial_reference as ref
    from threadpoolctl import threadpool_limits

    manifest, acct = _silver(spark, tmp_path)
    monkeypatch.setattr(ref, "CATALOG", "spark_catalog")
    monkeypatch.setattr(ref, "SILVER_TXNS", "refsilver.transactions")
    monkeypatch.setattr(ref, "SILVER_ENTITIES", "refsilver.entities")
    monkeypatch.setattr(ref, "SILVER_ACCOUNTS", "refsilver.accounts")
    monkeypatch.setattr(ref, "ACCOUNT_PATH", acct)
    with threadpool_limits(limits=2):
        report = ref.run_fidelity_gate(
            spark, manifest, cap_rows=10_000, provenance={"git_sha": "x"}
        )
    assert report["verdict"] == "ok"
    assert report["provenance"]["adapter"] == "silver"
    assert report["provenance"]["model_versions"] == ["datagen-v2-rs-0.2"]
    assert report["n_scored_customers"] == 120
    for t, r in report["typologies"].items():
        # 24 participants, half of them customers.
        assert r["n_positives"] == 12, t
        assert r["status"] == "ok" and r["underpowered"] is True
    assert report["density"]["planted_rows"] == 18
    assert report["level2"]["holds_on_this_corpus"] is False

    rows = ref._metric_rows(report)
    df = spark.createDataFrame(rows, ref._METRIC_SCHEMA)
    got = {r["typology_type"]: r for r in df.collect()}
    assert got[None]["row_kind"] == "aggregate"
    assert got["stack"]["ap"] == pytest.approx(report["typologies"]["stack"]["ap"])
    assert got["stack"]["n_positives"] == 12

    out = tmp_path / "aml_gate_report.json"
    ref._write_text(spark, f"file://{out}", json.dumps(report, default=str))
    assert json.loads(out.read_text())["verdict"] == "ok"


def test_cap_keeps_positives_and_weights_negatives(spark, tmp_path, monkeypatch):
    import aml_features as af
    import score_financial_reference as ref

    manifest, acct = _silver(spark, tmp_path)
    txns, ents, id_map = af.silver_frames(
        spark,
        catalog="spark_catalog",
        txns_table="refsilver.transactions",
        entities_table="refsilver.entities",
        accounts_table="refsilver.accounts",
        account_path=acct,
    )
    feats = af.entity_features(txns, ents)
    labels = af.labels_from_participants(manifest, id_map)
    pdf, n, frac = ref._cap_customers(feats, labels, ["stack", "rapid_layering"], cap_rows=40)
    assert n == 120 and 0 < frac < 1
    full, _, one = ref._cap_customers(feats, labels, ["stack", "rapid_layering"], cap_rows=10_000)
    assert one == 1.0 and len(full) == 120

    def positives(df):
        return df[(df["label:stack"] == 1) | (df["label:rapid_layering"] == 1)]

    pos = positives(pdf)
    # Every labelled customer survives the cap (the two instances overlap).
    assert len(pos) == len(positives(full)) == 18 and (pos["weight"] == 1.0).all()
    neg = pdf.drop(pos.index)
    assert len(neg) > 0 and all(w == pytest.approx(1 / frac) for w in neg["weight"])
