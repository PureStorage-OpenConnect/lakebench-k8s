"""The foreachBatch harness sets the two local properties the product reads
around the call and leaves the thread as it found it."""

from __future__ import annotations

import pytest
from _foreach_batch import BATCH_ID, QUERY_ID, foreach_batch_harness, inside_foreach_batch

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script")


def test_properties_set_inside_and_restored_after(spark_session):
    from common import streaming_query_id

    sc = spark_session.sparkContext
    seen = {}

    def handler(df, batch_id):
        seen["qid"] = streaming_query_id(spark_session)
        seen["bid"] = sc.getLocalProperty(BATCH_ID)
        return batch_id

    assert foreach_batch_harness(spark_session, handler, None, 7, query_id="q-1") == 7
    assert seen == {"qid": "q-1", "bid": "7"}
    assert sc.getLocalProperty(QUERY_ID) is None
    assert sc.getLocalProperty(BATCH_ID) is None
    with pytest.raises(RuntimeError, match="queryId is not set"):
        streaming_query_id(spark_session)


def test_outer_properties_survive_a_nested_call(spark_session):
    sc = spark_session.sparkContext
    with inside_foreach_batch(spark_session, 1, "outer"):
        with inside_foreach_batch(spark_session, 2, "inner"):
            assert sc.getLocalProperty(QUERY_ID) == "inner"
        assert sc.getLocalProperty(QUERY_ID) == "outer"
        assert sc.getLocalProperty(BATCH_ID) == "1"
    assert sc.getLocalProperty(QUERY_ID) is None


def test_restored_when_the_handler_raises(spark_session):
    def boom(_df, _bid):
        raise ValueError("handler failed")

    with pytest.raises(ValueError):
        foreach_batch_harness(spark_session, boom, None, 3)
    assert spark_session.sparkContext.getLocalProperty(QUERY_ID) is None
