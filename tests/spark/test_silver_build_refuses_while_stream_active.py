"""H4: silver_build_financial refuses to run while an AML stream is active.

The AML stream deployment writes a ``_STARTED`` marker file next to its
Structured Streaming checkpoint (``LB_FINANCIAL_SILVER_CHECKPOINT``) on
start-up and removes it on clean shutdown. If a fat-fingered operator
then invokes ``silver_build_financial`` on the same deployment, the batch
job would ``.overwrite(lit(True))`` silver.transactions / silver.entities /
silver.accounts / silver.account_statements / silver.counterparty_edges /
silver.entity_profiles, wiping every row the stream has written so far
(the docstring at ``silver_stream_financial.py:46-53`` calls this out).

The guard converts that docstring warning into a runtime refusal:

* Marker present + ``LB_FORCE_REBUILD`` NOT set -> ``SilverAbort`` naming
  the streaming checkpoint path.
* Marker present + ``LB_FORCE_REBUILD=1`` (or ``force_rebuild=True``)
  -> proceed silently. The operator explicitly asked for the wipe.
* Marker absent -> proceed silently. Fresh deployment, or the stream
  shut down cleanly.

The guard lives in ``common.refuse_batch_while_stream_active`` and is
called from ``silver_build_financial.main()`` right after the Spark
session is opened, before any DDL bootstrap or write happens. The tests
here exercise the helper via a fake sidecar filesystem (matching the B5
tests' ``_FakeSpark`` pattern) so no live Spark session is needed.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.usefixtures("load_script")


class _FakeFS:
    """In-memory Hadoop FileSystem for the marker path."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.writes = 0
        self.deletes = 0

    def exists(self, path):
        return str(path) in self.files

    def read(self, path):
        return self.files[str(path)]

    def write(self, path, data):
        self.files[str(path)] = data
        self.writes += 1

    def delete(self, path):
        if str(path) in self.files:
            del self.files[str(path)]
            self.deletes += 1
            return True
        return False


def test_refuses_with_silver_abort_when_marker_present():
    """Marker seeded, no force-rebuild: SilverAbort names the checkpoint path."""
    from common import SilverAbort, refuse_batch_while_stream_active

    fs = _FakeFS()
    ckpt = "s3a://lb-bronze/_checkpoints/silver_stream_financial/"
    marker = ckpt.rstrip("/") + "/_STARTED"
    # Seed the marker as the stream would have written on startup.
    fs.write(marker, b"pid=12345 stream_id=abc")

    with pytest.raises(SilverAbort) as excinfo:
        refuse_batch_while_stream_active(
            spark=None, checkpoint_location=ckpt, force_rebuild=False, fs=fs
        )
    msg = str(excinfo.value)
    # The refusal must name the streaming checkpoint path so the operator
    # sees exactly which deployment they are stepping on.
    assert ckpt.rstrip("/") in msg
    assert "_STARTED" in msg
    # And it must point the operator at the escape hatch.
    assert "force-rebuild" in msg.lower() or "force_rebuild" in msg.lower()


def test_force_rebuild_bypasses_marker_check():
    """Marker seeded, force_rebuild=True: no raise, guard returns cleanly."""
    from common import refuse_batch_while_stream_active

    fs = _FakeFS()
    ckpt = "s3a://lb-bronze/_checkpoints/silver_stream_financial/"
    marker = ckpt.rstrip("/") + "/_STARTED"
    fs.write(marker, b"pid=12345 stream_id=abc")

    # No exception: operator explicitly opted in to the wipe.
    refuse_batch_while_stream_active(
        spark=None, checkpoint_location=ckpt, force_rebuild=True, fs=fs
    )
    # The guard does NOT clear the marker: only the stream shutdown path
    # removes it, so a subsequent stream restart still sees a clean state
    # and the batch operator has not silently disabled the guard for
    # future runs.
    assert marker in fs.files


def test_main_calls_guard_before_writes(monkeypatch):
    """silver_build_financial.main wires the guard before any Spark writes.

    Mock the guard to raise a distinctive exception; assert main() surfaces
    it verbatim without ever reaching the DDL bootstrap or _replace_data
    stage. Also verifies main() reads LB_FINANCIAL_SILVER_CHECKPOINT and
    forwards LB_FORCE_REBUILD as the guard's force_rebuild argument.
    """
    pytest.importorskip("pyspark")

    import silver_build_financial as sbf

    class _Sentinel(RuntimeError):
        pass

    seen: dict[str, object] = {}

    def _fake_guard(*, spark, checkpoint_location, force_rebuild, fs=None):
        seen["spark"] = spark
        seen["checkpoint_location"] = checkpoint_location
        seen["force_rebuild"] = force_rebuild
        raise _Sentinel("guard fired")

    monkeypatch.setattr(sbf, "refuse_batch_while_stream_active", _fake_guard)

    # Point silver_build at a distinctive checkpoint so we can verify it
    # was forwarded. LB_FORCE_REBUILD=1 must propagate to the guard as
    # ``force_rebuild=True``.
    monkeypatch.setenv("LB_FINANCIAL_SILVER_CHECKPOINT", "s3a://fake/checkpoints/silver-h4/")
    monkeypatch.setenv("LB_FORCE_REBUILD", "1")

    # Stop main() from actually opening a Spark session or writing tables;
    # the guard is supposed to fire before anything is bootstrapped.
    class _StubSpark:
        class _Conf:
            def set(self, *a, **kw):
                pass

            def get(self, *a, **kw):
                return "UTC"

        conf = _Conf()

        def sql(self, *a, **kw):
            raise AssertionError("guard should have fired before any spark.sql call")

        def stop(self):
            pass

    class _Builder:
        def appName(self, *a, **kw):
            return self

        def getOrCreate(self):
            return _StubSpark()

    monkeypatch.setattr(sbf.SparkSession, "builder", _Builder())

    with pytest.raises(_Sentinel):
        sbf.main()

    assert seen["checkpoint_location"] == "s3a://fake/checkpoints/silver-h4/"
    assert seen["force_rebuild"] is True
