"""Config load warns when a continuous retention_threshold sits below the live floor."""

from __future__ import annotations

import yaml

from tests.conftest import make_config


def _cfg(mode="sustained", threshold=None, fmt="iceberg", engine="trino"):
    sustained = {} if threshold is None else {"retention_threshold": threshold}
    overrides = {
        "architecture": {
            "pipeline": {"mode": mode, "sustained": sustained},
            "table_format": {"type": fmt},
            "query_engine": {"type": engine},
        }
    }
    return make_config(**overrides)


def test_floor_matches_the_runtime_constant():
    from lakebench.config.loader import _ICEBERG_LIVE_EXPIRE_FLOOR_SECONDS
    from lakebench.modules.table_formats.iceberg.maintenance import (
        LIVE_EXPIRE_MIN_RETENTION_SECONDS,
    )

    assert _ICEBERG_LIVE_EXPIRE_FLOOR_SECONDS == LIVE_EXPIRE_MIN_RETENTION_SECONDS


def test_default_30m_in_continuous_mode_warns():
    from lakebench.config.loader import load_advisories

    msgs = load_advisories(_cfg())
    assert len(msgs) == 1
    assert "30m" in msgs[0] and "1h floor" in msgs[0]


def test_at_or_above_floor_is_silent():
    from lakebench.config.loader import load_advisories

    assert load_advisories(_cfg(threshold="1h")) == []
    assert load_advisories(_cfg(threshold="60m")) == []
    assert load_advisories(_cfg(threshold="2d")) == []


def test_batch_config_is_silent_at_load():
    """Saved configs carry every field, so a batch config never warns at load."""
    from lakebench.config.loader import load_advisories, retention_floor_advisory

    assert load_advisories(_cfg(mode="batch")) == []
    assert load_advisories(_cfg(mode="batch", threshold="10m")) == []
    # The continuous loop still warns when a batch config runs --sustained.
    assert retention_floor_advisory(_cfg(mode="batch", threshold="10m"))


def test_continuous_loop_warns_for_a_batch_config_run_sustained():
    import inspect

    import lakebench.cli._sustained as sus

    src = inspect.getsource(sus._run_sustained)
    assert "floor_msg = retention_floor_advisory(cfg)" in src
    assert "print_warning(floor_msg)" in src


def test_silent_where_continuous_maintenance_does_not_run():
    """Delta (no effective continuous maintenance), DuckDB and none skip it."""
    from lakebench.config.loader import load_advisories

    assert load_advisories(_cfg(threshold="10m", fmt="delta")) == []
    assert load_advisories(_cfg(threshold="10m", engine="duckdb")) == []
    assert load_advisories(_cfg(threshold="10m", engine="none")) == []
    assert len(load_advisories(_cfg(threshold="10m", engine="spark-thrift"))) == 1


def test_load_config_prints_the_warning_to_stderr(tmp_path, capsys):
    from lakebench.config.loader import load_config

    cfg = _cfg()
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg.model_dump(mode="json")))
    import lakebench.config.loader as loader

    loader._printed_advisories.clear()
    load_config(path)
    load_config(path)
    captured = capsys.readouterr()
    assert captured.err.count("retention_threshold is 30m") == 1  # once per process
    assert "retention_threshold" not in captured.out
