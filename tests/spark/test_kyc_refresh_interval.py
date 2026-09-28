"""I7: silver_stream_financial._kyc re-reads the KYC masters when the
LB_STREAM_KYC_REFRESH_SECONDS interval has elapsed since the last load.

A load emits a `kyc_refreshed_at` metric via log_job_metrics so operators
can see the refresh cadence in metrics.json.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def _reset_state(mod):
    mod._KYC = None
    mod._KYC_LOADED = False
    mod._KYC_LOADED_AT = 0.0


def test_reloads_when_interval_elapses(spark, monkeypatch):
    """3 micro-batches over a 5s interval: initial load + one refresh = 2 loads.

    Clock advances 3s between batches 0 and 1 (below interval, no refresh),
    then 3s more between batches 1 and 2 (crosses interval since batch 0's
    load timestamp, one refresh). reference_frames is the read that actually
    hits the object store, so its call count is the loaded-twice signal.
    """
    import silver_stream_financial as ss

    _reset_state(ss)
    monkeypatch.setattr(ss, "KYC_REFRESH_S", 5)

    clock = {"now": 0.0}
    monkeypatch.setattr(ss.time, "time", lambda: clock["now"])

    party = spark.createDataFrame(
        [(1, "US", True, True)], "entity_id long, ctry string, a boolean, b boolean"
    )
    account = spark.createDataFrame(
        [(11, "US01", 1)], "account_id long, iban string, holder_entity_id long"
    )

    reference_calls: list[float] = []

    def _refs(_spark):
        reference_calls.append(clock["now"])
        return party, account

    monkeypatch.setattr(ss, "reference_frames", _refs)

    # build_kyc is called with the loaded frames; a marker DataFrame is enough.
    kyc_stub = spark.createDataFrame([(1,)], "entity_id long")
    monkeypatch.setattr(ss, "build_kyc", lambda _p, _a: kyc_stub)

    # Capture kyc_refreshed_at emits.
    refreshed_calls: list[int] = []
    real_emit = ss.log_job_metrics

    def _emit(job, **kw):
        if "kyc_refreshed_at" in kw:
            refreshed_calls.append(int(kw["kyc_refreshed_at"]))
        real_emit(job, **kw)

    monkeypatch.setattr(ss, "log_job_metrics", _emit)

    # Batch 0 at t=0: initial load.
    clock["now"] = 0.0
    assert ss._kyc(spark) is kyc_stub
    assert reference_calls == [0.0]
    assert refreshed_calls == [0]

    # Batch 1 at t=3: below the 5s interval, use cached KYC.
    clock["now"] = 3.0
    assert ss._kyc(spark) is kyc_stub
    assert reference_calls == [0.0]
    assert refreshed_calls == [0]

    # Batch 2 at t=6: (6-0) crosses the interval, reload and re-emit.
    clock["now"] = 6.0
    assert ss._kyc(spark) is kyc_stub
    assert reference_calls == [0.0, 6.0]
    assert refreshed_calls == [0, 6]

    # Sanity: state visible on the module reflects the last load.
    assert ss._KYC_LOADED is True
    assert ss._KYC_LOADED_AT == 6.0


def test_refresh_keeps_cache_on_transient_read_failure(spark, monkeypatch):
    """A mid-stream reference read that returns None or raises must not stall
    the micro-batch on the KYC_WAIT_S wait loop: the initial load already
    succeeded, so the cached frame is served and the next micro-batch retries."""
    import silver_stream_financial as ss

    _reset_state(ss)
    monkeypatch.setattr(ss, "KYC_REFRESH_S", 5)

    clock = {"now": 0.0}
    monkeypatch.setattr(ss.time, "time", lambda: clock["now"])

    party = spark.createDataFrame([(1,)], "entity_id long")
    account = spark.createDataFrame(
        [(11, "US01", 1)], "account_id long, iban string, holder_entity_id long"
    )
    kyc_stub = spark.createDataFrame([(1,)], "entity_id long")
    monkeypatch.setattr(ss, "build_kyc", lambda _p, _a: kyc_stub)

    calls: list[str] = []

    def _refs_ok(_spark):
        calls.append("ok")
        return party, account

    def _refs_none(_spark):
        calls.append("none")
        return None, None

    def _refs_raise(_spark):
        calls.append("raise")
        raise RuntimeError("s3 transient")

    # Sleep must not be called during a refresh path (would block the batch).
    slept: list[float] = []
    monkeypatch.setattr(ss.time, "sleep", lambda s: slept.append(s))

    # Initial load succeeds.
    monkeypatch.setattr(ss, "reference_frames", _refs_ok)
    clock["now"] = 0.0
    first = ss._kyc(spark)
    assert first is kyc_stub

    # Refresh due, but reference_frames returns (None, None): keep cache.
    monkeypatch.setattr(ss, "reference_frames", _refs_none)
    clock["now"] = 10.0
    got = ss._kyc(spark)
    assert got is kyc_stub  # same cached frame
    # _KYC_LOADED_AT unchanged so the next batch retries.
    assert ss._KYC_LOADED_AT == 0.0
    # No sleep (no wait loop entered on refresh).
    assert slept == []

    # Refresh due, reference_frames raises: keep cache.
    monkeypatch.setattr(ss, "reference_frames", _refs_raise)
    clock["now"] = 20.0
    got = ss._kyc(spark)
    assert got is kyc_stub
    assert ss._KYC_LOADED_AT == 0.0
    assert slept == []

    # Refresh due, reference_frames recovers: reload succeeds.
    monkeypatch.setattr(ss, "reference_frames", _refs_ok)
    clock["now"] = 30.0
    got = ss._kyc(spark)
    assert got is kyc_stub
    assert ss._KYC_LOADED_AT == 30.0
    assert slept == []

    assert calls == ["ok", "none", "raise", "ok"]


def test_refresh_interval_env_default(monkeypatch):
    """Default LB_STREAM_KYC_REFRESH_SECONDS is 3600.

    A fresh import of the module without the env var picks 3600; setting
    the env var changes the module-level constant on re-import.
    """
    import importlib

    import silver_stream_financial as ss

    monkeypatch.delenv("LB_STREAM_KYC_REFRESH_SECONDS", raising=False)
    reloaded = importlib.reload(ss)
    assert reloaded.KYC_REFRESH_S == 3600
    monkeypatch.setenv("LB_STREAM_KYC_REFRESH_SECONDS", "17")
    reloaded = importlib.reload(ss)
    assert reloaded.KYC_REFRESH_S == 17
    # Reset back to the default so other tests are not affected.
    monkeypatch.delenv("LB_STREAM_KYC_REFRESH_SECONDS", raising=False)
    importlib.reload(ss)
