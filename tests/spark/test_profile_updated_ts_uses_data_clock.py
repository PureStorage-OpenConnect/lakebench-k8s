"""I1 (silver-plan): build_entity_profiles emits ``profile_updated_ts``
derived from the resolved data clock, not from wall-clock.

Rationale: rebuilding entity_profiles with ``current_timestamp()`` makes
every silver rebuild write a different value into a per-entity column,
so a byte-identical silver rebuild becomes impossible even when the
input bronze has not changed. Deriving ``profile_updated_ts`` from the
resolved data clock keeps rebuilds byte-identical for a fixed bronze.

The first two tests are text checks; the signature and runtime tests
import the script, which needs pyspark, so the file is in the Spark tier
(CI's unit legs have no pyspark and skip the whole module).
"""

from __future__ import annotations

from pathlib import Path

import pytest

# CI's unit legs collect tests/spark without pyspark: skip, do not error in
# the spark_session fixture.
pytest.importorskip("pyspark")

_SCRIPTS = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"

pytestmark = pytest.mark.usefixtures("load_script")


def test_build_entity_profiles_uses_data_clock_expression(spark_session):
    """Runtime: the emitted profile_updated_ts is the passed clock's
    midnight in the session time zone (UTC, as the product sets it).
    Read back as a string in that zone: ``collect()`` would convert the
    instant to the host's local time and move the day on a host west of
    UTC."""
    from datetime import date, datetime

    import silver_build_financial as sbf
    from pyspark.sql import Row

    assert spark_session.conf.get("spark.sql.session.timeZone") == "UTC"
    rows = [
        Row(
            originator_id="E1",
            beneficiary_id="E2",
            txn_timestamp=datetime(2024, 3, 1, 0, 0, 0),
            txn_amount_usd=100.0,
        ),
        Row(
            originator_id="E2",
            beneficiary_id="E1",
            txn_timestamp=datetime(2024, 4, 1, 0, 0, 0),
            txn_amount_usd=50.0,
        ),
    ]
    txns = spark_session.createDataFrame(rows)
    clock = date(2025, 6, 15)
    profiles = sbf.build_entity_profiles(txns, data_clock=clock)
    got = [
        r["ts"] for r in profiles.selectExpr("CAST(profile_updated_ts AS STRING) AS ts").collect()
    ]
    assert got, "expected at least one profile row"
    assert set(got) == {"2025-06-15 00:00:00"}, got
