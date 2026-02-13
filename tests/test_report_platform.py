"""Tests for report generator platform section (P4).

Covers:
- _generate_platform_section() with various inputs
- _generate_html() platform_metrics parameter
"""

from __future__ import annotations

from lakebench.reports.generator import ReportGenerator

# ===========================================================================
# _generate_platform_section
# ===========================================================================


class TestGeneratePlatformSection:
    """Tests for ReportGenerator._generate_platform_section()."""

    def test_none_returns_empty(self):
        gen = ReportGenerator()
        assert gen._generate_platform_section(None) == ""

    def test_empty_dict_returns_empty(self):
        gen = ReportGenerator()
        assert gen._generate_platform_section({}) == ""

    def test_empty_pods_no_error_returns_empty(self):
        """No pods and no collection_error should return empty."""
        gen = ReportGenerator()
        data = {
            "pods": [],
            "collection_error": None,
            "duration_seconds": 0,
            "s3_requests_total": 0,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 0,
        }
        assert gen._generate_platform_section(data) == ""

    def test_with_pods(self):
        gen = ReportGenerator()
        data = {
            "pods": [
                {
                    "pod_name": "lakebench-trino-coord-0",
                    "component": "trino-coordinator",
                    "cpu_avg_cores": 1.5,
                    "cpu_max_cores": 2.0,
                    "memory_avg_bytes": 2 * 1024**3,
                    "memory_max_bytes": 3 * 1024**3,
                },
                {
                    "pod_name": "lakebench-duckdb-abc",
                    "component": "duckdb",
                    "cpu_avg_cores": 0.5,
                    "cpu_max_cores": 1.0,
                    "memory_avg_bytes": 1 * 1024**3,
                    "memory_max_bytes": 2 * 1024**3,
                },
            ],
            "s3_requests_total": 150,
            "s3_errors_total": 5,
            "s3_avg_latency_ms": 12.3,
            "duration_seconds": 600,
            "collection_error": None,
        }
        html = gen._generate_platform_section(data)
        assert "Platform Metrics" in html
        assert "trino-coordinator" in html
        assert "duckdb" in html
        assert "150" in html  # s3 requests
        assert "600" in html  # duration
        assert "12.3" in html  # latency

    def test_with_collection_error(self):
        gen = ReportGenerator()
        data = {
            "pods": [],
            "s3_requests_total": 0,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 0,
            "duration_seconds": 0,
            "collection_error": "Connection refused to prometheus:9090",
        }
        html = gen._generate_platform_section(data)
        assert "Connection refused" in html
        assert "Platform Metrics" in html

    def test_s3_summary_cards(self):
        gen = ReportGenerator()
        data = {
            "pods": [
                {
                    "pod_name": "pod-1",
                    "component": "trino-worker",
                    "cpu_avg_cores": 1.0,
                    "cpu_max_cores": 1.5,
                    "memory_avg_bytes": 1e9,
                    "memory_max_bytes": 2e9,
                }
            ],
            "s3_requests_total": 999,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 5.5,
            "duration_seconds": 300,
            "collection_error": None,
        }
        html = gen._generate_platform_section(data)
        assert "S3 Requests" in html
        assert "999" in html
        assert "S3 Errors" in html
        assert "S3 Avg Latency" in html

    def test_memory_conversion_to_gib(self):
        """Memory values should be converted to GiB in the display."""
        gen = ReportGenerator()
        data = {
            "pods": [
                {
                    "pod_name": "pod-1",
                    "component": "trino-coordinator",
                    "cpu_avg_cores": 1.0,
                    "cpu_max_cores": 2.0,
                    "memory_avg_bytes": 4 * 1024**3,  # 4 GiB
                    "memory_max_bytes": 8 * 1024**3,  # 8 GiB
                }
            ],
            "s3_requests_total": 0,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 0,
            "duration_seconds": 300,
            "collection_error": None,
        }
        html = gen._generate_platform_section(data)
        assert "4.0 GiB" in html
        assert "8.0 GiB" in html

    def test_cpu_values_displayed(self):
        gen = ReportGenerator()
        data = {
            "pods": [
                {
                    "pod_name": "pod-1",
                    "component": "duckdb",
                    "cpu_avg_cores": 1.55,
                    "cpu_max_cores": 2.10,
                    "memory_avg_bytes": 1e9,
                    "memory_max_bytes": 2e9,
                }
            ],
            "s3_requests_total": 0,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 0,
            "duration_seconds": 60,
            "collection_error": None,
        }
        html = gen._generate_platform_section(data)
        assert "1.55" in html
        assert "2.10" in html

    def test_table_headers_present(self):
        gen = ReportGenerator()
        data = {
            "pods": [
                {
                    "pod_name": "p",
                    "component": "c",
                    "cpu_avg_cores": 0,
                    "cpu_max_cores": 0,
                    "memory_avg_bytes": 0,
                    "memory_max_bytes": 0,
                }
            ],
            "s3_requests_total": 0,
            "s3_errors_total": 0,
            "s3_avg_latency_ms": 0,
            "duration_seconds": 0,
            "collection_error": None,
        }
        html = gen._generate_platform_section(data)
        assert "Pod" in html
        assert "Component" in html
        assert "CPU Avg" in html
        assert "CPU Max" in html
        assert "Mem Avg" in html
        assert "Mem Max" in html
