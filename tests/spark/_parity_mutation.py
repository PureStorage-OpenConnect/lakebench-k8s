"""Test-only shim for the parity mutation check (design 04 V16-2).

``install()`` does nothing unless ``LB_PARITY_MUTATE`` names a mutation. It
then replaces ``common.materialised_source`` with a wrapper that, for the
one MERGE source the mutation names, replaces one column with a NULL of the
column's type before the source is materialised. The four protocol guards'
Spark children call ``install()`` before they import the stream script (the
script binds ``materialised_source`` at import), so a guard run with the
variable set compares a stream whose MERGE saw that column nulled.

- ``LB_PARITY_MUTATE``: a key of ``MUTATIONS``.
- ``LB_PARITY_MUTATE_RUN``: optional; mutate only while the micro-batch
  thread's job group (the stream run id) equals it, so a replay guard can
  mutate the replayed run only.
- ``LB_PARITY_MUTATE_RECORD``: optional file; one JSON line per mutated
  source with its row count and the AM-15a fingerprint before and after, so
  the caller can see the mutation ran and changed the rows.

A named column missing from the source raises: a mutation that cannot apply
must not let a guard pass or fail for another reason.
"""

from __future__ import annotations

import json
import os
from typing import Any

# Mutation key -> (view-name prefix of the MERGE source, source column).
MUTATIONS = {
    # Site B: silver.entities MERGE; the stream keeps country NULL.
    "entities.country": ("_silver_stream_dim_merge_entities", "country"),
    # Site A: current_balance roll-up; the target column takes NULL.
    "accounts.current_balance": ("_d_full_balances_", "running_balance"),
    # Site E: profiles MERGE; first_seen_ts is LEAST(target, source).
    "profiles.first_seen_ts": ("_lb_profiles_delta", "batch_first_seen_ts"),
}


def install() -> None:
    key = os.environ.get("LB_PARITY_MUTATE")
    if not key:
        return
    if key not in MUTATIONS:
        raise ValueError(f"LB_PARITY_MUTATE={key!r} is not one of {sorted(MUTATIONS)}")
    prefix, column = MUTATIONS[key]
    run = os.environ.get("LB_PARITY_MUTATE_RUN")
    record = os.environ.get("LB_PARITY_MUTATE_RECORD")

    import common

    real = common.materialised_source

    class mutated_source(real):  # type: ignore[misc,valid-type]  # noqa: N801
        def __init__(self, spark: Any, df: Any, view_name: str) -> None:
            if view_name.startswith(prefix) and _in_run(spark, run):
                df = _null_column(spark, df, column, key, view_name, record)
            super().__init__(spark, df, view_name)

    common.materialised_source = mutated_source


def _in_run(spark: Any, run: str | None) -> bool:
    if run is None:
        return True
    return spark.sparkContext.getLocalProperty("spark.jobGroup.id") == run


def _null_column(spark: Any, df: Any, column: str, key: str, view: str, record: str | None) -> Any:
    from common import frame_fingerprint
    from pyspark.sql.functions import lit

    fields = {f.name: f.dataType for f in df.schema.fields}
    if column not in fields:
        raise RuntimeError(f"parity mutation {key}: {view} has no column {column!r}")
    mutated = df.withColumn(column, lit(None).cast(fields[column]))
    if record:
        cols = list(fields)
        before = frame_fingerprint(df, cols)
        after = frame_fingerprint(mutated, cols)
        with open(record, "a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "mutation": key,
                        "view": view,
                        "rows": before[0],
                        "fp_before": before[1],
                        "fp_after": after[1],
                    }
                )
                + "\n"
            )
    return mutated
