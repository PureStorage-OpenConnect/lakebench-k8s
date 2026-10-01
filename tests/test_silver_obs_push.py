"""Unit tests for the bronze/silver/gold stage metrics push added to
spark/scripts/common.py:log_job_metrics (Gate 2 observability). The push is
best-effort: no-op without LB_PUSHGATEWAY_URL, correct exposition payload with
it, and per-silver-table row counts become a labeled gauge.
"""

from __future__ import annotations

import socket
import threading
from pathlib import Path

import pytest

_SCRIPTS = str(Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts")


@pytest.fixture
def common(load_script):
    return load_script("common")


def _one_shot_listener():
    """Bind an ephemeral TCP port; return (port, get_request) where get_request
    blocks for one connection and returns the raw request bytes."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    captured = {}

    def serve():
        conn, _ = srv.accept()
        conn.settimeout(3)
        data = b""
        try:
            while b"\r\n\r\n" not in data:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                data += chunk
            header, _, body = data.partition(b"\r\n\r\n")
            clen = 0
            for line in header.split(b"\r\n"):
                if line.lower().startswith(b"content-length:"):
                    clen = int(line.split(b":", 1)[1])
            while len(body) < clen:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                body += chunk
            data = header + b"\r\n\r\n" + body
        except Exception:
            pass
        conn.sendall(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
        conn.close()
        srv.close()
        captured["req"] = data.decode("utf-8", "replace")

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    return port, t, captured


def test_no_op_when_env_unset(common, monkeypatch):
    monkeypatch.delenv("LB_PUSHGATEWAY_URL", raising=False)
    # Must not raise and must not attempt any network call.
    common._push_stage_metrics(
        "silver_build",
        input_size_gb=1.0,
        input_rows=10,
        output_rows=9,
        elapsed_seconds=5.0,
        extra={},
    )


def test_push_payload_and_url(common, monkeypatch):
    port, t, captured = _one_shot_listener()
    monkeypatch.setenv("LB_PUSHGATEWAY_URL", f"http://127.0.0.1:{port}")
    monkeypatch.setenv("LB_RUN_ID", "run-xyz")
    common._push_stage_metrics(
        "silver_build",
        input_size_gb=2.5,
        input_rows=100,
        output_rows=95,
        elapsed_seconds=12.0,
        extra={"silver_transactions_rows": 42, "kyc_refresh_kind": "full"},
    )
    t.join(timeout=5)
    req = captured.get("req", "")
    assert req.startswith("PUT /metrics/job/spark_stage/stage/silver_build/run_id/run-xyz HTTP/1.1")
    assert "lakebench_stage_input_rows 100" in req
    assert "lakebench_stage_output_rows 95" in req
    assert "lakebench_stage_elapsed_seconds 12.0" in req
    # Per-silver-table count becomes a labeled gauge; non-_rows extras are ignored.
    assert 'lakebench_silver_table_rows{table="transactions"} 42' in req
    assert "kyc_refresh_kind" not in req


def test_push_failure_is_swallowed(common, monkeypatch):
    # Env set but nothing listening: must not raise (best-effort).
    monkeypatch.setenv("LB_PUSHGATEWAY_URL", "http://127.0.0.1:1")
    common._push_stage_metrics(
        "bronze_verify",
        input_size_gb=0.0,
        input_rows=0,
        output_rows=0,
        elapsed_seconds=0.1,
        extra={},
    )
