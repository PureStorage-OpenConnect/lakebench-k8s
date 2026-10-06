"""FQ4 and IQ3 give one answer per corpus, in batch and continuous silver.

Continuous AML writes silver.counterparty_edges as one row per pair per
micro-batch (batch writes one per pair) and stores
silver.account_statements running balances in arrival order. FQ4 used to
return the stored bal_after and IQ3's second hop the raw edge rows, so the
two modes, and any two continuous runs with different micro-batch
boundaries, answered differently on one corpus. FQ4 now recomputes the
running balance in ledger order and IQ3 sums both hops per pair.

One corpus of eleven payments between five parties goes through:

* continuous: three micro-batches through
  ``silver_stream_financial._merge_batch`` (the product's write path),
  the second one earlier in time than the first (a late arrival), so
  account statements land in arrival order and the A->B and A->C edges are
  split over several rows;
* batch: ``silver_build_financial.build_statements`` over the whole corpus
  and ``build_edges`` over the same transactions (one row per pair).

Checked, each query run through the Spark Thrift dialect adapter as the
benchmark runs it:

* on batch silver the new FQ4 and IQ3 return exactly what the previous SQL
  returned (batch answers unchanged);
* on continuous silver the new queries return the batch answer;
* on continuous silver the previous SQL does not (the fixture really has
  arrival-order balances and split edges).
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
from datetime import timedelta
from decimal import Decimal

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]

# The SQL of query set qs12-4bd2d9416abb, before this change.
OLD_FQ4 = """\
WITH top_accts AS (
  SELECT account_id
  FROM {catalog}.{silver_account_statements}
  GROUP BY account_id
  ORDER BY COUNT(*) DESC, account_id
  LIMIT 50
)
SELECT
  s.account_id,
  s.book_ts,
  s.cdt_dbt_ind,
  s.amt,
  s.bal_after,
  ROW_NUMBER() OVER (PARTITION BY s.account_id ORDER BY s.book_ts, s.entry_seq) AS entry_ord
FROM {catalog}.{silver_account_statements} s
JOIN top_accts t ON t.account_id = s.account_id
ORDER BY s.account_id, entry_ord"""

OLD_IQ3_HOP2 = """hop2 AS (
  SELECT h.cp AS via_entity_id, e.target_entity_id AS hop2_entity_id,
         e.cumulative_amount_usd AS amount_usd
  FROM {catalog}.{silver_counterparty_edges} e
  JOIN hop1 h ON e.source_entity_id = h.cp
),"""


def test_fq4_and_iq3_answer_the_same_in_batch_and_continuous(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=900)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    for q in ("FQ4", "IQ3"):
        # Non-degenerate: the fixture produces rows for both queries.
        assert out[q]["new_batch"], (q, out[q])
        # Batch answers unchanged.
        assert out[q]["new_batch"] == out[q]["old_batch"], (q, out[q])
        # Continuous gives the batch answer.
        assert out[q]["new_stream"] == out[q]["new_batch"], (q, out[q])
        # The fixture exercises the layout difference the old SQL read.
        assert out[q]["old_stream"] != out[q]["old_batch"], (q, out[q])
    assert out["stream_edge_rows"] > out["batch_edge_rows"], out
    assert out["late_ibans"] > 0, out


def _run(jars):
    from _d_full_helpers import (
        BASE_TS,
        EDGES_DDL,
        PACS_SCHEMA,
        STATEMENTS_DDL,
        bind_stream_module,
        bootstrap_catalog,
        build_spark,
        seed_account,
    )

    from lakebench.benchmark.queries import INVESTIGATOR_QUERIES, get_benchmark_queries
    from lakebench.config.schema import WorkloadSchema
    from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor

    iban = {p: f"GB{i:02d}PARTY{p}" for i, p in enumerate("SABCD")}

    def row(txn_id, hours, dbtr, cdtr, amt):
        def party(nm):
            return (f"PARTY {nm}", "GB", ("LONDON", "HIGH ST"), (f"LEI-{nm}",))

        return (
            txn_id,
            f"UETR-{txn_id}",
            party(dbtr),
            party(cdtr),
            ("MERIUS2L",),
            ("NRTHGB3X",),
            (iban[dbtr],),
            (iban[cdtr],),
            (None,),
            (None,),
            (None,),
            Decimal(amt),
            "USD",
            BASE_TS + timedelta(hours=hours),
            "SALA",
            [],
            f"MSG-{txn_id}",
        )

    batches = [
        [
            row("T01", 10, "S", "A", "100.00"),
            row("T02", 11, "A", "B", "40.00"),
            row("T03", 12, "A", "C", "30.00"),
            row("T04", 13, "B", "D", "5.00"),
        ],
        # Late: earlier than every row of the first micro-batch.
        [row("T11", 1, "S", "A", "50.00"), row("T12", 2, "A", "B", "20.00")],
        [
            row("T21", 20, "A", "B", "10.00"),
            row("T22", 21, "A", "C", "7.00"),
            row("T23", 22, "C", "A", "3.00"),
            row("T24", 23, "D", "S", "2.00"),
            row("T25", 23, "S", "S", "1.00"),
        ],
    ]

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jars)
        bootstrap_catalog(spark)
        for p in "SABCD":
            seed_account(spark, iban[p])

        # --- continuous silver, the product's micro-batch path.
        ss = bind_stream_module(spark)
        spark.sparkContext.setJobGroup("stream-run-1", "test")
        late = 0
        for bid, rows in enumerate(batches):
            result = foreach_batch_harness(
                spark, ss._merge_batch, spark.createDataFrame(rows, PACS_SCHEMA), bid
            )
            late += int(result[1])

        # --- batch silver over the same corpus.
        import silver_build_financial as sbf

        spark.sql(STATEMENTS_DDL.replace("lh.silver.account_statements", "lh.silver.stmts_batch"))
        bronze_all = spark.createDataFrame([r for b in batches for r in b], PACS_SCHEMA)
        sbf.build_statements(bronze_all, spark.table("lh.silver.accounts")).writeTo(
            "lh.silver.stmts_batch"
        ).append()
        spark.sql(EDGES_DDL.replace("lh.silver.counterparty_edges", "lh.silver.edges_batch"))
        sbf.build_edges(spark.table("lh.silver.transactions")).writeTo(
            "lh.silver.edges_batch"
        ).append()

        # The oldest open case is S's; B is alerted.
        ids = {
            r["txn_id"]: (r["originator_id"], r["beneficiary_id"])
            for r in spark.table("lh.silver.transactions").collect()
        }
        s_id, a_id = ids["T01"]
        b_id = ids["T02"][1]
        spark.sql(
            "CREATE TABLE lh.silver.cases (case_id STRING, customer_id BIGINT, "
            "base_run_id STRING, case_status STRING, opened_date DATE) USING iceberg"
        )
        spark.sql(
            f"INSERT INTO lh.silver.cases VALUES ('c1', {s_id}, 'run-1', 'open', DATE'2024-06-02')"
        )
        spark.sql(
            "CREATE TABLE lh.silver.dispositions (entity_id BIGINT, base_run_id STRING) USING iceberg"
        )
        spark.sql(f"INSERT INTO lh.silver.dispositions VALUES ({b_id}, 'run-1')")

        adapt = SparkThriftExecutor.__new__(SparkThriftExecutor).adapt_query
        by_name = {q.name: q.sql for q in get_benchmark_queries(WorkloadSchema.FINANCIAL)}
        iq3 = next(q.sql for q in INVESTIGATOR_QUERIES if q.name == "IQ3_counterparty_two_hop")
        old_iq3, n = re.subn(r"hop2 AS \(.*?\n\),", OLD_IQ3_HOP2, iq3, count=1, flags=re.S)
        assert n == 1
        sql = {
            "FQ4": {"new": by_name["FQ4_running_balance_window"], "old": OLD_FQ4},
            "IQ3": {"new": iq3, "old": old_iq3},
        }
        tables = {
            "batch": ("silver.stmts_batch", "silver.edges_batch"),
            "stream": ("silver.account_statements", "silver.counterparty_edges"),
        }

        def answer(text, mode):
            stmts, edges = tables[mode]
            q = text.format(
                catalog="lh",
                silver_account_statements=stmts,
                silver_counterparty_edges=edges,
                silver_entities="silver.entities",
                gold_cases="silver.cases",
                gold_alert_dispositions="silver.dispositions",
                tm_run_id="run-1",
            )
            return [[str(v) for v in r] for r in spark.sql(adapt(q)).collect()]

        out = {
            q: {f"{v}_{m}": answer(sql[q][v], m) for v in ("new", "old") for m in tables}
            for q in sql
        }
        out["stream_edge_rows"] = spark.table("lh.silver.counterparty_edges").count()
        out["batch_edge_rows"] = spark.table("lh.silver.edges_batch").count()
        out["late_ibans"] = late
        out["a_is_hop1"] = str(a_id)
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts, tests/spark and src on
    # PYTHONPATH and passes the jar classpath.
    _run(sys.argv[1])
