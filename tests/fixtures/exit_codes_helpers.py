"""Shared test helpers moved from tests/test_exit_codes.py (imported by several test files)."""

from __future__ import annotations

from pathlib import Path


def _reproduce_package(tmp_path, commit_sha: str) -> Path:
    import yaml

    from lakebench.cli._reproduce import _build_package
    from tests.fixtures.reproduce_helpers import _ONE_SAMPLE_CFG, _metrics

    (tmp_path / "cfg.yaml").write_text(_ONE_SAMPLE_CFG)
    pkg = _build_package(_metrics(), config_reference="cfg.yaml", commit_sha=commit_sha)
    path = tmp_path / "pkg.yaml"
    path.write_text(yaml.safe_dump(pkg))
    return path
