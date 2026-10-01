"""H4 fail-open: silver_build_financial proceeds when no marker exists.

The stream ``_STARTED`` marker is written by silver_stream_financial on
start-up and removed on clean shutdown. On a fresh AML deployment that
has never run continuous mode, the marker (and, for older deployments,
the whole checkpoint directory) does not exist. Silver-batch is
legitimate in that state and MUST proceed -- refusing it would break
every batch-only deployment on first run.

The fail-open contract:

* Checkpoint directory does not exist -> proceed.
* Checkpoint directory exists but no ``_STARTED`` file -> proceed.
* Any transient filesystem failure while probing the marker -> proceed
  (log warning). H4 is defense-in-depth on top of operational hygiene,
  not the only guard, so a broken FS lookup must not stall a legitimate
  batch run.

The tests exercise the helper via a fake sidecar filesystem so no live
Spark session is needed.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.usefixtures("load_script")


class _FakeFS:
    def __init__(self, *, exists_raises=False):
        self.files: dict[str, bytes] = {}
        self.writes = 0
        self.deletes = 0
        self._exists_raises = exists_raises

    def exists(self, path):
        if self._exists_raises:
            raise RuntimeError("simulated FS listing failure")
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


def test_no_marker_no_refusal():
    """Empty FS: guard returns cleanly, no raise, no side effects."""
    from common import refuse_batch_while_stream_active

    fs = _FakeFS()
    ckpt = "s3a://lb-bronze/_checkpoints/silver_stream_financial/"

    # No exception. Legitimate batch on a stream-free deployment.
    refuse_batch_while_stream_active(
        spark=None, checkpoint_location=ckpt, force_rebuild=False, fs=fs
    )
    # The guard MUST NOT create the marker itself: it is a stream-side
    # writer only. A silver-batch run that touched the marker would
    # confuse the next stream startup (which reads the marker to detect
    # a prior unclean shutdown -- see B5 for the sidecar precedent).
    assert fs.files == {}
    assert fs.writes == 0
    assert fs.deletes == 0


def test_fs_probe_failure_fails_open(caplog):
    """Transient FS listing failure -> log warning, do not raise."""
    from common import refuse_batch_while_stream_active

    fs = _FakeFS(exists_raises=True)
    ckpt = "s3a://lb-bronze/_checkpoints/silver_stream_financial/"

    with caplog.at_level("WARNING"):
        # Must not raise: defense-in-depth guard, not a hard gate. A
        # broken S3 listing during startup would otherwise convert every
        # transient outage into a silver-batch outage.
        refuse_batch_while_stream_active(
            spark=None, checkpoint_location=ckpt, force_rebuild=False, fs=fs
        )


def test_none_checkpoint_falls_open():
    """Guard tolerates a caller that never set the checkpoint env var.

    A batch-only deployment may not export ``LB_FINANCIAL_SILVER_CHECKPOINT``
    at all; silver_build_financial passes whatever it resolves. None or
    empty means "no known stream", so the guard fails open. This keeps
    the fresh-deployment path free of a required env var.
    """
    from common import refuse_batch_while_stream_active

    # Neither value should raise. force_rebuild flag has no effect on
    # the fail-open path.
    refuse_batch_while_stream_active(
        spark=None, checkpoint_location=None, force_rebuild=False, fs=_FakeFS()
    )
    refuse_batch_while_stream_active(
        spark=None, checkpoint_location="", force_rebuild=False, fs=_FakeFS()
    )
