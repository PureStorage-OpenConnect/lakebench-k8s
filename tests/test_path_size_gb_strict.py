"""A6 (silver-plan): path_size_gb_strict propagates listing failures.

The non-strict ``path_size_gb`` swallows every exception and returns 0.0,
which made an S3 outage indistinguishable from a truly empty bronze path
(a silver run then exit-1'd through the "bronze empty" branch instead of
being retried at the operator level). The strict variant re-raises, so a
listing failure surfaces as a driver error the K8s Job restart policy or
the operator can act on. The non-strict wrapper stays available for the
metrics-only callers that do not need the distinction.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"))

from common import path_size_gb, path_size_gb_strict  # noqa: E402


class _RaisingPath:
    """A ``Path``-like object whose file system raises on any listing call."""

    def __init__(self, exc):
        self._exc = exc

    def getFileSystem(self, _hconf):  # noqa: N802 (JVM API)
        raise self._exc


class _RaisingJvm:
    def __init__(self, exc):
        self._exc = exc

        class _Fs:
            class _Path:
                pass

        class _Hadoop:
            class _Fs:
                class _Path:
                    pass

            class fs:  # noqa: N801
                class Path:
                    def __init__(inner, uri):  # noqa: N805
                        pass

                    def getFileSystem(inner, _hconf):  # noqa: N802,N805
                        raise exc

        # jvm.org.apache.hadoop.fs.Path(uri)
        self.org = SimpleNamespace(
            apache=SimpleNamespace(
                hadoop=_Hadoop,
            )
        )


def _fake_spark(exc):
    """A stub Spark that hands out a Hadoop Path whose FS raises ``exc``."""

    class _JavaSc:
        def hadoopConfiguration(self):  # noqa: N802
            return object()

    return SimpleNamespace(_jvm=_RaisingJvm(exc), _jsc=_JavaSc())


def test_path_size_gb_strict_reraises_listing_failure():
    """The strict variant must not swallow the listing exception."""
    exc = RuntimeError("simulated S3 500 while listing s3a://lb-bronze/")
    spark = _fake_spark(exc)
    with pytest.raises(RuntimeError, match="simulated S3 500"):
        path_size_gb_strict(spark, "s3a://lb-bronze/customer/interactions")


def test_path_size_gb_swallows_listing_failure():
    """The non-strict variant preserves the existing 0.0-on-error contract.

    Metrics-only callers rely on 0.0 as "unknown size" without failing the run.
    This test guards against a regression where the wrapper starts propagating
    and takes down callers that never asked to be strict.
    """
    exc = RuntimeError("simulated S3 500 while listing s3a://lb-bronze/")
    spark = _fake_spark(exc)
    assert path_size_gb(spark, "s3a://lb-bronze/customer/interactions") == 0.0
