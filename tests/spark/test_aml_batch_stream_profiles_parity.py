"""D-full-profiles: batch and stream must produce equivalent
silver.entity_profiles for the same bronze.

Batch mode computes the aggregates in one pass:
    stddev = sample stddev over originator amounts
    _m2 = variance * (n - 1)
Stream mode maintains the same aggregates incrementally via the Welford
parallel merge. The two paths must converge byte-identically on additive,
LEAST/GREATEST and derived columns; ``stddev_amount_usd`` and ``_m2`` are
allowed a small floating-point tolerance (Welford accumulates rounding
error linearly in the number of merges; a strict byte-identical assertion
would reject a numerically-correct implementation).

Any regression that lets batch and stream diverge -- for example, a
Welford implementation that drops the ``delta^2 * n_a * n_b / n``
cross-term, or a MERGE UPDATE SET that references the post-update t.x
instead of the pre-update value -- fails at least one of the exact-match
assertions below.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")

pytestmark = [
    pytest.mark.requires_jars("iceberg"),
    # The module-scoped fixture below runs both scripts once for every test.
    pytest.mark.usefixtures("load_script_module"),
    # One core, as before the shared harness: the profile sums are compared
    # with a tolerance, and merge order follows the partition count.
    pytest.mark.spark_static_conf({"spark.master": "local[1]"}),
]

# The Iceberg catalog the scripts are pointed at (LB_ICEBERG_CATALOG) and the
# one registered on the shared session.
_CATALOG = "lh"


_PACS_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string>, cdtr_acct struct<iban:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)


def _party(nm):
    return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))


def _row(txn_id, orig, ben, ts, amount):
    return (
        txn_id,
        f"UETR-{txn_id}",
        _party(orig),
        _party(ben),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01",),
        ("GB02",),
        (None,),
        (None,),
        (None,),
        Decimal(amount),
        "USD",
        ts,
        "SALA",
        [],
        f"MSG-{txn_id}",
    )


@pytest.fixture(scope="module", autouse=True)
def _catalog_env():
    """Point the stream / build modules at the test's Iceberg catalog
    before a test imports them: their DDL literals interpolate
    ``{CATALOG}`` at import, so patching module attributes after import is
    too late."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LB_ICEBERG_CATALOG", _CATALOG)
        yield


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(
        spark_session, _CATALOG, tmp_path_factory.mktemp("aml-parity-wh"), cache_enabled=False
    )
    return spark_session


@pytest.fixture(scope="module")
def profiles(spark, load_script_module):
    return build_profiles(spark)


def build_profiles(spark):
    """(batch rows, stream rows) of silver.entity_profiles by entity_id,
    for one bronze corpus built once by each path, in catalog lh. Run by
    the fixture above and, as a Spark child, by the parity mutation check
    (tests/spark/test_parity_guard_mutations.py)."""
    import silver_stream_financial as ss

    # Build a shared bronze corpus: 13 transactions among six entities;
    # amounts spread enough that stddev is non-zero. A, Z, B and Y both send
    # and receive (T9 to T11); C only sends; R only receives, in both
    # micro-batches, so its never-used send side meets the UPDATE branch.
    base = datetime(2024, 6, 1)
    batch0 = [
        _row("T1", "A", "Z", base + timedelta(days=0), "100.00"),
        _row("T2", "A", "Z", base + timedelta(days=1), "150.00"),
        _row("T3", "A", "Y", base + timedelta(days=2), "200.00"),
        _row("T4", "B", "Z", base + timedelta(days=3), "50.00"),
        _row("T5", "B", "Y", base + timedelta(days=4), "75.00"),
        _row("T12", "A", "R", base + timedelta(days=4, hours=6), "20.00"),
    ]
    batch1 = [
        _row("T6", "B", "Z", base + timedelta(days=5), "300.00"),
        _row("T7", "C", "Z", base + timedelta(days=6), "125.00"),
        _row("T8", "C", "Y", base + timedelta(days=7), "175.00"),
        _row("T9", "Z", "A", base + timedelta(days=8), "60.00"),
        _row("T10", "Y", "B", base + timedelta(days=9), "90.00"),
        _row("T11", "Z", "Y", base + timedelta(days=10), "45.00"),
        _row("T13", "C", "R", base + timedelta(days=11), "30.00"),
    ]
    corpus = batch0 + batch1
    bronze = spark.createDataFrame(corpus, _PACS_SCHEMA)

    # --- Batch path: build_entity_profiles directly.
    from datetime import date

    import silver_build_financial as sbf

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver_batch")
    spark.sql(sbf.DDL_PROFILES.replace("silver.entity_profiles", "silver_batch.entity_profiles"))

    from pyspark.sql.functions import lit as _lit

    batch_txns = sbf.build_transactions(bronze)
    batch_profiles = sbf.build_entity_profiles(batch_txns, data_clock=date(2024, 7, 1))
    batch_profiles.writeTo("lh.silver_batch.entity_profiles").overwrite(_lit(True))

    # --- Stream path: feed the same corpus as two micro-batches through
    # _merge_batch (proxy for the streaming trigger; the MERGE code path
    # is identical), with the foreachBatch local properties set
    # (_foreach_batch). Batch 0 inserts A, B, Y, Z and R; batch 1 inserts C
    # and updates the other five. In the UPDATE, the
    # Welford arms run as: B sends in both batches (the general arm with the
    # cross-term); Z only received in batch 0 and sends twice in batch 1
    # (the receive-then-send arm, and stddev at exactly two sends); A sends
    # only in batch 0 (the keep arm).
    #
    # The stream writes the tables its DDL literals name, which interpolate
    # the table names at import: the default silver.* names in catalog lh
    # (_catalog_env). Reassigning ss.SILVER_* after import would point the
    # MERGE at tables the DDLs never created. The batch side writes only
    # lh.silver_batch.entity_profiles, so the two never share a table.
    assert ss.CATALOG == _CATALOG
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    for ddl_attr in (
        "DDL_TXNS",
        "DDL_ENTITIES",
        "DDL_ACCOUNTS",
        "DDL_STATEMENTS",
        "DDL_EDGES",
        "DDL_PROFILES",
        "DDL_BATCH_VERSIONS",
    ):
        spark.sql(getattr(ss, ddl_attr))
    ss._KYC = None
    ss._KYC_LOADED = True
    ss._kyc = lambda _s: None
    ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)
    foreach_batch_harness(spark, ss._merge_batch, spark.createDataFrame(batch0, _PACS_SCHEMA), 0)
    foreach_batch_harness(spark, ss._merge_batch, spark.createDataFrame(batch1, _PACS_SCHEMA), 1)

    # --- Compare per-entity rows.
    batch_rows = {
        r["entity_id"]: r for r in spark.table("lh.silver_batch.entity_profiles").collect()
    }
    stream_rows = {r["entity_id"]: r for r in spark.table(f"lh.{ss.SILVER_PROFILES}").collect()}
    # Six entities on each side.
    assert len(batch_rows) == 6, sorted(batch_rows)
    return batch_rows, stream_rows


def coverage_problems(batch_rows, stream_rows):
    if set(batch_rows) == set(stream_rows):
        return []
    return [f"entity coverage drift: batch={set(batch_rows)}, stream={set(stream_rows)}"]


def sum_problems(batch_rows, stream_rows):
    """total_sent_usd and total_received_usd, Decimal(38,2), compared
    exactly as amounts, with NULL read as 0.00 (null_problems compares the
    NULLs)."""
    zero = Decimal("0.00")
    out = []
    for eid, b in batch_rows.items():
        s = stream_rows.get(eid)
        if s is None:
            continue  # coverage_problems names it
        for c in ("total_sent_usd", "total_received_usd"):
            b_v = zero if b[c] is None else b[c]
            s_v = zero if s[c] is None else s[c]
            if b_v != s_v:
                out.append(f"{eid}.{c}: batch={b[c]}, stream={s[c]}")
    return out


def null_problems(batch_rows, stream_rows):
    """A side the entity never used is NULL in both paths."""
    out = []
    for eid, b in batch_rows.items():
        s = stream_rows.get(eid)
        if s is None:
            continue
        for c in ("total_sent_usd", "total_received_usd"):
            if (b[c] is None) != (s[c] is None):
                out.append(f"{eid}.{c}: batch={b[c]}, stream={s[c]}")
    return out


def _close(b, s):
    if b is None or s is None:
        return b is None and s is None
    return math.isclose(float(b), float(s), rel_tol=1e-9, abs_tol=1e-9)


def value_problems(batch_rows, stream_rows):
    """Every other compared column: counts exact, LEAST/GREATEST timestamps
    exact, the derived columns, the Welford accumulators and the mean within
    a tight tolerance (incremental merges accrue rounding error compared
    with one pass)."""
    out = []
    for eid, b in batch_rows.items():
        s = stream_rows.get(eid)
        if s is None:
            continue
        # Additive and count columns, LEAST / GREATEST timestamps, and
        # active_span_days (DATEDIFF, integer as DOUBLE): exact.
        for c in (
            "txn_count_out",
            "txn_count_in",
            "txn_count_total",
            "distinct_counterparties_out",
            "distinct_counterparties_in",
            "first_seen_ts",
            "last_seen_ts",
            "active_span_days",
        ):
            if b[c] != s[c]:
                out.append(f"{eid}.{c}: batch={b[c]}, stream={s[c]}")
        # W4 reads passthrough_ratio and W8 avg_gap_days; both are derived.
        for c in ("passthrough_ratio", "avg_gap_days", "stddev_amount_usd", "avg_amount_usd"):
            if not _close(b[c], s[c]):
                out.append(f"{eid}.{c}: batch={b[c]}, stream={s[c]}")
        if not _close(float(b["_m2"] or 0.0), float(s["_m2"] or 0.0)):
            out.append(f"{eid}._m2: batch={b['_m2']}, stream={s['_m2']}")
    return out


def test_batch_and_stream_profile_the_same_entities(profiles):
    assert not coverage_problems(*profiles)


def test_batch_and_stream_profile_sums_match(profiles):
    problems = sum_problems(*profiles)
    assert not problems, problems


def test_batch_and_stream_profile_sums_agree_on_null(profiles):
    problems = null_problems(*profiles)
    assert not problems, problems


def test_batch_and_stream_produce_equivalent_profiles(profiles):
    problems = value_problems(*profiles)
    assert not problems, problems


def _child(jars):
    """Spark child for the mutation check: the same corpus and comparison,
    with the parity mutation shim installed before the scripts load."""
    import json
    import os
    import tempfile

    os.environ["LB_ICEBERG_CATALOG"] = _CATALOG
    import _parity_mutation

    _parity_mutation.install()
    from _d_full_helpers import build_spark

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jars)
        batch_rows, stream_rows = build_profiles(spark)
        out = {
            "coverage": coverage_problems(batch_rows, stream_rows),
            "sums": sum_problems(batch_rows, stream_rows),
            "nulls": null_problems(batch_rows, stream_rows),
            "values": value_problems(batch_rows, stream_rows),
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    import sys

    _child(sys.argv[1])
