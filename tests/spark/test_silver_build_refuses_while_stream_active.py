"""silver_build_financial refuses to run while an AML stream is active.

The stream writes a ``_STARTED`` marker next to its checkpoint
(``LB_FINANCIAL_SILVER_CHECKPOINT``) on start-up and removes it on clean
shutdown. A batch run on the same deployment would overwrite every silver
table and wipe the rows the stream wrote, so the guard
``common.refuse_batch_while_stream_active`` behaves as follows:

* Marker present, no force rebuild: ``SilverAbort``.
* Marker present, force rebuild: proceed, marker left in place.
* Marker absent, checkpoint missing or unset, or the marker probe fails:
  proceed (fail open), so a batch-only deployment is never blocked.

The helper is exercised through a fake sidecar filesystem, so no live Spark
session is needed.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.usefixtures("load_script")

CKPT = "s3a://lb-bronze/_checkpoints/silver_stream_financial/"
MARKER = CKPT.rstrip("/") + "/_STARTED"


class _FakeFS:
    """In-memory marker filesystem; ``exists`` can be made to fail."""

    def __init__(self, files=(), *, exists_raises=False):
        self.files = set(files)
        self._exists_raises = exists_raises

    def exists(self, path):
        if self._exists_raises:
            raise RuntimeError("simulated FS listing failure")
        return str(path) in self.files


def test_refuses_with_silver_abort_when_marker_present():
    from common import SilverAbort, refuse_batch_while_stream_active

    fs = _FakeFS([MARKER])
    with pytest.raises(SilverAbort) as excinfo:
        refuse_batch_while_stream_active(
            spark=None, checkpoint_location=CKPT, force_rebuild=False, fs=fs
        )
    assert CKPT.rstrip("/") in str(excinfo.value)


def test_force_rebuild_bypasses_marker_check():
    from common import refuse_batch_while_stream_active

    fs = _FakeFS([MARKER])
    refuse_batch_while_stream_active(
        spark=None, checkpoint_location=CKPT, force_rebuild=True, fs=fs
    )
    # Only the stream shutdown path removes the marker.
    assert fs.files == {MARKER}


@pytest.mark.parametrize(
    ("checkpoint", "fs"),
    [
        (CKPT, _FakeFS()),
        (CKPT, _FakeFS(exists_raises=True)),
        (None, _FakeFS([MARKER])),
        ("", _FakeFS([MARKER])),
    ],
    ids=["no_marker", "probe_failure", "checkpoint_unset", "checkpoint_empty"],
)
def test_guard_fails_open(checkpoint, fs):
    from common import refuse_batch_while_stream_active

    before = set(fs.files)
    refuse_batch_while_stream_active(
        spark=None, checkpoint_location=checkpoint, force_rebuild=False, fs=fs
    )
    # The marker is a stream-side writer only: the guard never creates or removes it.
    assert fs.files == before


def test_main_calls_guard_before_writes(monkeypatch):
    """main() reads LB_FINANCIAL_SILVER_CHECKPOINT, forwards LB_FORCE_REBUILD
    as ``force_rebuild`` and calls the guard before any Spark write."""
    pytest.importorskip("pyspark")

    import silver_build_financial as sbf

    class _Sentinel(RuntimeError):
        pass

    seen: dict[str, object] = {}

    def _fake_guard(*, spark, checkpoint_location, force_rebuild, fs=None):
        seen["checkpoint_location"] = checkpoint_location
        seen["force_rebuild"] = force_rebuild
        raise _Sentinel("guard fired")

    class _StubSpark:
        def __init__(self):
            self.conf = self

        def set(self, *a, **kw):
            pass

        def get(self, *a, **kw):
            return "UTC"

        def sql(self, *a, **kw):
            raise AssertionError("guard should have fired before any spark.sql call")

        def stop(self):
            pass

    class _Builder:
        def appName(self, *a, **kw):
            return self

        def getOrCreate(self):
            return _StubSpark()

    monkeypatch.setattr(sbf, "refuse_batch_while_stream_active", _fake_guard)
    monkeypatch.setattr(sbf.SparkSession, "builder", _Builder())
    monkeypatch.setenv("LB_FINANCIAL_SILVER_CHECKPOINT", "s3a://fake/checkpoints/silver-h4/")
    monkeypatch.setenv("LB_FORCE_REBUILD", "1")

    with pytest.raises(_Sentinel):
        sbf.main()

    assert seen["checkpoint_location"] == "s3a://fake/checkpoints/silver-h4/"
    assert seen["force_rebuild"] is True
