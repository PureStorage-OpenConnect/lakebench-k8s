"""S3 operation metrics wrapper for Lakebench.

Wraps ``S3Client`` method calls with timing and counting metrics.
These are CLI-side boto3 operations only -- NOT Spark/Trino data path I/O.

Metrics are exposed via ``prometheus_client`` when ``observability.enabled``
is true and ``prometheus_client`` is installed; otherwise this module is a
transparent no-op wrapper.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Lazy-import prometheus_client so it remains an optional dependency.
_prom_available = False
try:
    from prometheus_client import Counter, Histogram

    _prom_available = True
except ImportError:
    pass


class S3MetricsWrapper:
    """Wraps an S3Client instance with timing/counting metrics.

    If ``prometheus_client`` is not installed, all operations pass through
    without instrumentation.
    """

    def __init__(self, s3_client: Any, enabled: bool = True):
        self._client = s3_client
        self._enabled = enabled and _prom_available

        if self._enabled:
            self._request_duration = Histogram(
                "lakebench_s3_request_duration_seconds",
                "Duration of S3 API requests made by the lakebench CLI",
                labelnames=["operation"],
            )
            self._requests_total = Counter(
                "lakebench_s3_requests_total",
                "Total S3 API requests made by the lakebench CLI",
                labelnames=["operation"],
            )
            self._errors_total = Counter(
                "lakebench_s3_errors_total",
                "Total S3 API errors encountered by the lakebench CLI",
                labelnames=["operation"],
            )

    def _wrap(self, operation: str, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn* and record metrics."""
        if not self._enabled:
            return fn(*args, **kwargs)

        self._requests_total.labels(operation=operation).inc()
        start = time.monotonic()
        try:
            result = fn(*args, **kwargs)
            return result
        except Exception:
            self._errors_total.labels(operation=operation).inc()
            raise
        finally:
            elapsed = time.monotonic() - start
            self._request_duration.labels(operation=operation).observe(elapsed)

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the underlying S3Client.

        Methods that look like S3 operations get automatically wrapped.
        """
        attr = getattr(self._client, name)
        if callable(attr) and not name.startswith("_"):

            def wrapped(*args: Any, **kwargs: Any) -> Any:
                return self._wrap(name, attr, *args, **kwargs)

            return wrapped
        return attr
