"""Call a stream's micro-batch handler the way ``foreachBatch`` does.

Inside a real ``foreachBatch`` Spark sets, on the micro-batch thread, the
local properties ``sql.streaming.queryId`` and ``streaming.sql.batchId``
and the job group (the run id). The product reads the query id
(``common.streaming_query_id``) and refuses to run without it. A test that
calls the handler directly sets the first two around the call and restores
what was there before, so the product guard stays as it is. The run id is
the test's to set (``setJobGroup``): without one, ``replay_possible``
treats every batch as a possible replay, the safe fallback.

Local properties belong to the calling thread (pyspark pins each Python
thread to its own JVM thread), so a test with several writer threads
enters ``inside_foreach_batch`` in each thread.

Imported by tests and by Spark children (tests/spark is on their
PYTHONPATH), so it does not import pyspark or pytest.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

QUERY_ID = "sql.streaming.queryId"
BATCH_ID = "streaming.sql.batchId"
# One query id for every call, as one streaming query restarted from its
# checkpoint keeps its id.
DEFAULT_QUERY_ID = "lb-test-query"


@contextmanager
def inside_foreach_batch(
    spark: Any, batch_id: int, query_id: str = DEFAULT_QUERY_ID
) -> Iterator[None]:
    sc = spark.sparkContext
    before = {key: sc.getLocalProperty(key) for key in (QUERY_ID, BATCH_ID)}
    sc.setLocalProperty(QUERY_ID, query_id)
    sc.setLocalProperty(BATCH_ID, str(int(batch_id)))
    try:
        yield
    finally:
        for key, value in before.items():
            # None removes the property.
            sc.setLocalProperty(key, value)


def foreach_batch_harness(
    spark: Any,
    fn: Callable[[Any, int], Any],
    df: Any,
    batch_id: int,
    query_id: str = DEFAULT_QUERY_ID,
) -> Any:
    """``fn(df, batch_id)`` with the foreachBatch local properties set."""
    with inside_foreach_batch(spark, batch_id, query_id):
        return fn(df, batch_id)
