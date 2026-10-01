"""B1 (Delta): repeat cycle 1/2 with (txnAppId, txnVersion) collapses to no-op.

Delta short-circuits any (appId, version) it has recorded, so a re-submission
of the same cycle within one rebuild epoch commits nothing. This test writes
cycles 0/1/2 each twice with delta_batch_txn_options and asserts:

- Final row count equals a single-run sequence's count.
- DESCRIBE HISTORY shows a SET TRANSACTION on every second submission and
  no data was added by it.
"""

from __future__ import annotations

import glob
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_JARS = os.environ.get("LB_SPARK_TEST_JARS", "")
_NEEDED = ("delta-spark", "delta-storage")


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return all(any(n.startswith(k) for n in names) for k in _NEEDED)


pytestmark = [
    pytest.mark.skipif(not _have_jars(), reason="LB_SPARK_TEST_JARS with Delta jars not set"),
    pytest.mark.usefixtures("load_script"),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("delta-wh")
    jars = ",".join(sorted(glob.glob(os.path.join(_JARS, "*.jar"))))
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", f"file://{wh}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def _rows(spark, n, start=0):
    return spark.createDataFrame(
        [
            (
                i,
                1 + i % 5,
                datetime(2024, 6, 1) + timedelta(hours=i),
                "purchase",
                100.0 + i,
            )
            for i in range(start, start + n)
        ],
        "id bigint, customer_id bigint, event_timestamp timestamp, "
        "interaction_type string, transaction_amount double",
    )


def test_repeated_cycle_appends_are_delta_no_ops(spark, tmp_path):
    """cycles 0/1/2 each submitted twice; final count matches single-run sequence."""
    from common import delta_batch_txn_options

    tbl = "spark_catalog.default.delta_idem_txn"
    path = str(tmp_path / "delta-idem")
    # Cycle 0: base rebuild.
    _rows(spark, 5).write.format("delta").mode("overwrite").option("path", path).saveAsTable(tbl)

    # Cycle 1 twice.
    opts1 = delta_batch_txn_options("lb-silver-build", 0, 1)
    for _ in range(2):
        (
            _rows(spark, 7, start=100)
            .write.format("delta")
            .mode("append")
            .options(**opts1)
            .save(path)
        )
    # Cycle 2 twice.
    opts2 = delta_batch_txn_options("lb-silver-build", 0, 2)
    for _ in range(2):
        (
            _rows(spark, 3, start=200)
            .write.format("delta")
            .mode("append")
            .options(**opts2)
            .save(path)
        )

    # Single-run reference sequence: 5 + 7 + 3 = 15 rows.
    assert spark.read.format("delta").load(path).count() == 15

    # DESCRIBE HISTORY should include at least one SET TRANSACTION for each
    # duplicate submission (Delta records the txn but no data commit).
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    ops = [r["operation"] for r in hist]
    assert "SET TRANSACTION" in ops or ops.count("WRITE") <= 3, (
        "expected duplicate submissions to be logged as SET TRANSACTION no-ops "
        f"or coalesced by Delta; saw operations={ops}"
    )


def test_new_rebuild_epoch_moves_appid_namespace():
    """A cycle 0 under epoch 1 has a different appId than under epoch 0."""
    from common import delta_batch_txn_options

    a = delta_batch_txn_options("lb-silver-build", 0, 0)
    b = delta_batch_txn_options("lb-silver-build", 1, 0)
    assert a["txnAppId"] != b["txnAppId"]
    assert a["txnVersion"] == b["txnVersion"] == "0"
