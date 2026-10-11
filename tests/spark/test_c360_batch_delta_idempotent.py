"""Delta: repeating cycle 1 or 2 with (txnAppId, txnVersion) collapses to a no-op.

Delta short-circuits any (appId, version) it has recorded, so a re-submission
of the same cycle within one rebuild epoch commits nothing. The test writes
cycles 0/1/2, the later two each twice, with delta_batch_txn_options and
asserts the final row count equals a single-run sequence's count.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script")


@pytest.fixture(scope="module")
def spark(spark_session):
    """The shared session: Delta extension, DeltaCatalog as spark_catalog."""
    return spark_session


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


@pytest.mark.requires_jars("delta")
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
