"""The S3 endpoint value is redacted in metrics.json and report.html (LB-280).

report.html is shared off the operator's host as the human-readable artefact
and metrics.json is checked into uat/runs/. Neither should carry the real
endpoint value. The system_identity fingerprint keeps per-endpoint
distinguishability by hashing the host:port.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from lakebench.metrics.collector import _redact_endpoint, build_config_snapshot
from lakebench.metrics.system_identity import _storage_endpoint
from lakebench.reports.generator import ReportGenerator
from tests.conftest import make_config

# A unique, syntactically-valid S3 endpoint that is NOT the lab IP. The IP
# in this literal (TEST-NET-1, RFC 5737) is reserved for documentation /
# examples and will not appear in a real deployment config.
_RAW = "http://192.0.2.123:80"
_RAW_HOST = "192.0.2.123"


def _make_cfg_with_endpoint(endpoint: str = _RAW):
    cfg = make_config()
    cfg.platform.storage.s3.endpoint = endpoint
    return cfg


def test_redact_endpoint_hashes_the_value():
    out = _redact_endpoint(_RAW)
    assert out is not None
    assert _RAW_HOST not in out
    assert out.startswith("s3-endpoint-")
    # 16 hex chars after the prefix
    assert re.fullmatch(r"s3-endpoint-[0-9a-f]{16}", out)


def test_redact_endpoint_is_stable_per_input():
    assert _redact_endpoint(_RAW) == _redact_endpoint(_RAW)
    assert _redact_endpoint("http://other.example:9000") != _redact_endpoint(_RAW)


def test_redact_endpoint_passes_through_empty():
    assert _redact_endpoint(None) is None
    assert _redact_endpoint("") == ""


def test_config_snapshot_does_not_leak_endpoint():
    cfg = _make_cfg_with_endpoint()
    snap = build_config_snapshot(cfg)
    serialised = json.dumps(snap)
    assert _RAW_HOST not in serialised, "config_snapshot leaks the raw S3 endpoint value (LB-280)"
    # the s3.endpoint field is still populated, just redacted
    assert snap["s3"]["endpoint"] is not None
    assert snap["s3"]["endpoint"].startswith("s3-endpoint-")


def test_system_identity_storage_endpoint_is_hashed():
    cfg = _make_cfg_with_endpoint()
    val = _storage_endpoint(cfg)
    assert isinstance(val, str)
    assert _RAW_HOST not in val
    assert val.startswith("s3-endpoint-")
    # Different endpoints distinguishable
    cfg2 = _make_cfg_with_endpoint("http://other.example:9000")
    assert _storage_endpoint(cfg2) != val


def test_report_html_does_not_render_s3_endpoint_field(tmp_path: Path):
    """The S3 Endpoint field was removed from the config-item list (LB-280).

    The reader does not need the operator's endpoint to interpret results; the
    storage backend type lives in system_identity.parts.storage_backend.
    """

    class _StubMetrics:
        def __init__(self, snap):
            self.config_snapshot = snap

    cfg = _make_cfg_with_endpoint()
    snap = build_config_snapshot(cfg)
    gen = ReportGenerator()
    html = gen._generate_config_section(_StubMetrics(snap))  # type: ignore[attr-defined]
    assert "S3 Endpoint" not in html, (
        "report.html still renders the 'S3 Endpoint' field; LB-280 is open again"
    )
    # Defence in depth: even if the config_snapshot somehow carried the raw
    # value, it must not appear in the rendered HTML.
    assert _RAW_HOST not in html
