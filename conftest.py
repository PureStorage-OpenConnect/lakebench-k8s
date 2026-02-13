"""Pytest configuration for lakebench."""

# Prevent collection from source tree
collect_ignore = ["src"]


def pytest_configure(config):
    """Register custom test markers."""
    config.addinivalue_line("markers", "unit: Unit tests (no external dependencies, fast)")
    config.addinivalue_line(
        "markers", "integration: Integration tests (require K8s/S3 connectivity)"
    )
    config.addinivalue_line("markers", "e2e: End-to-end tests (full deploy/run/destroy workflow)")
    config.addinivalue_line("markers", "slow: Slow tests (skip with -m 'not slow')")
    config.addinivalue_line("markers", "extended: Extended scale tests (scales 1, 10, 50, 100)")
    config.addinivalue_line("markers", "stress: Stress tests at large scales (250, 500, 1000)")
