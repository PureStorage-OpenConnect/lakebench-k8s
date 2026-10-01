"""D-full-profiles: replaying a batch already recorded in
silver.batch_versions must leave silver.entity_profiles unchanged.

The stream micro-batch handler folds new Welford + additive deltas into
silver.entity_profiles. Without an idempotency check, a retry from the
same checkpoint would apply the same delta twice and inflate the
accumulators. The lane wires in a subset of I10's silver_batch_versions
sidecar: replay_possible + (stream_id, batch_id) hit -> skip the MERGE
and leave the profile row untouched.

The test simulates that: apply a first batch (which records the version
row), then invoke _merge_batch again in a new query run against the same
input. The second call must see replay_possible=True and skip the
profiles MERGE. silver.transactions rows are DELETEed and re-INSERTed by
_merge_batch's own I5 pattern, so their count stays constant; profiles
must remain identical.
"""

from __future__ import annotations

import glob
import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

# Point the stream module at the test's Iceberg catalog before import
# (module DDL literals interpolate `{CATALOG}` at import time).
os.environ.setdefault("LB_ICEBERG_CATALOG", "lh")

import pytest

pytest.importorskip("pyspark")

_JARS = os.environ.get("LB_SPARK_TEST_JARS", "")


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return any(n.startswith("iceberg-spark-runtime") for n in names)


pytestmark = [
    pytest.mark.skipif(not _have_jars(), reason="LB_SPARK_TEST_JARS with Iceberg jars not set"),
    pytest.mark.usefixtures("load_script"),
]


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


def _bronze(spark):
    def party(nm):
        return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))

    rows = [
        (
            "T1",
            "UETR-T1",
            party("A"),
            party("Z"),
            ("MERIUS2L",),
            ("NRTHGB3X",),
            ("US01",),
            ("GB02",),
            (None,),
            (None,),
            (None,),
            Decimal("100.00"),
            "USD",
            datetime(2024, 6, 1),
            "SALA",
            [],
            "MSG-T1",
        ),
        (
            "T2",
            "UETR-T2",
            party("A"),
            party("Z"),
            ("MERIUS2L",),
            ("NRTHGB3X",),
            ("US01",),
            ("GB02",),
            (None,),
            (None,),
            (None,),
            Decimal("200.00"),
            "USD",
            datetime(2024, 6, 2),
            "SALA",
            [],
            "MSG-T2",
        ),
    ]
    return spark.createDataFrame(rows, _PACS_SCHEMA)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("aml-replay-wh")
    jars = ",".join(sorted(glob.glob(os.path.join(_JARS, "*.jar"))))
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lh.type", "hadoop")
        .config("spark.sql.catalog.lh.cache-enabled", "false")
        .config("spark.sql.catalog.lh.warehouse", f"file://{wh}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_profiles_replay_is_self_idempotent_on_stream_id_batch_id(spark):
    """Replaying the same (stream_id, batch_id) leaves silver.entity_profiles
    byte-identical. The self-idempotent MERGE gates a re-apply via a
    ``WHEN MATCHED AND (t._stream_id = s.batch_stream_id AND t._batch_id
    = s.batch_batch_id) THEN UPDATE SET entity_id = entity_id`` no-op
    branch, so a driver crash between the profiles commit and I10's
    sealed-marker commit does not double-count on retry.

    I10's silver_batch_versions is the shared sidecar; this test uses
    its shape but does not depend on the sealed marker being written,
    because the phase-4 MERGE's self-idempotency is what actually stops
    the double-count.
    """
    import silver_stream_financial as ss
    from common import _RUNS_STARTED

    ss.CATALOG = "lh"
    ss.SILVER_TXNS = "silver.transactions"
    ss.SILVER_EDGES = "silver.counterparty_edges"
    ss.SILVER_ENTITIES = "silver.entities"
    ss.SILVER_ACCOUNTS = "silver.accounts"
    ss.SILVER_PROFILES = "silver.entity_profiles"
    ss.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"
    ss.streaming_query_id = lambda _s: "qid-replay"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    for ddl in (
        ss.DDL_TXNS,
        ss.DDL_ENTITIES,
        ss.DDL_ACCOUNTS,
        ss.DDL_STATEMENTS,
        ss.DDL_EDGES,
        ss.DDL_PROFILES,
        ss.DDL_BATCH_VERSIONS,
    ):
        spark.sql(ddl)
    ss._KYC = None
    ss._KYC_LOADED = True
    ss._kyc = lambda _s: None
    ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)

    # First apply: fresh run id 'run-1' -> replay_possible=True.
    spark.sparkContext.setJobGroup("run-1", "test")
    _RUNS_STARTED.clear()
    ss._merge_batch(_bronze(spark), 0)
    first = spark.table("lh.silver.entity_profiles").orderBy("entity_id").collect()

    # Second apply: NEW run id -> replay_possible=True again. The MERGE
    # runs but its guarded WHEN MATCHED branch fires (same _stream_id,
    # _batch_id already on the target rows) and emits a no-op UPDATE, so
    # additive counters do not double.
    spark.sparkContext.setJobGroup("run-2", "test")
    _RUNS_STARTED.clear()
    ss._merge_batch(_bronze(spark), 0)
    second = spark.table("lh.silver.entity_profiles").orderBy("entity_id").collect()

    assert first == second, (
        f"profiles must be byte-identical on replay; got before={first}, after={second}"
    )
    # Sealed marker sidecar: phase 5 MERGE (WHEN NOT MATCHED INSERT) is
    # itself idempotent; exactly one row per sealed batch.
    ver_rows_after = spark.table(f"lh.{ss.SILVER_BATCH_VERSIONS}").collect()
    assert len(ver_rows_after) == 1, (
        f"replay must not append a second sealed-marker row; got {ver_rows_after}"
    )
