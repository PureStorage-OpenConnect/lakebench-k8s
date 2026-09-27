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


def test_explicit_30m_in_continuous_mode_warns():
    from lakebench.config.loader import load_advisories

    msgs = load_advisories(_cfg(threshold="30m"))
    assert len(msgs) == 1
    assert "30m" in msgs[0] and "1h floor" in msgs[0]


def test_default_30m_is_silent_and_the_floor_is_recorded():
    """lb16-checks2: a defaults-only continuous config warned on every run and
    destroy. The default is not the user's choice; the run records what ran."""
    from lakebench.cli._sustained import continuous_retention_record
    from lakebench.config.loader import load_advisories

    cfg = _cfg()
    assert load_advisories(cfg) == []
    assert continuous_retention_record(cfg) == {
        "configured": "30m",
        "configured_by": "default",
        "applied_expire": "1h",
        "applied_orphan": "1450m",
    }


def test_default_is_silent_through_load_config(tmp_path, capsys):
    from lakebench.config.loader import load_config

    path = tmp_path / "c.yaml"
    full = _cfg().model_dump(mode="json")
    minimal = {
        "name": full["name"],
        "platform": full["platform"],
        "architecture": {
            "pipeline": {"mode": "continuous"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "trino"},
        },
    }
    path.write_text(yaml.safe_dump(minimal))
    import lakebench.config.loader as loader

    loader._printed_advisories.clear()
    cfg = load_config(path)
    assert cfg.architecture.pipeline.sustained.retention_threshold == "30m"
    assert "retention_threshold" not in capsys.readouterr().err


def test_applied_retentions_match_the_policy():
    from lakebench.cli._sustained import applied_retentions

    assert applied_retentions("iceberg", "30m", live_streams=True) == {
        "expire": "1h",
        "orphan": "1450m",
    }
    assert applied_retentions("iceberg", "30m", live_streams=False)["expire"] == "30m"
    assert applied_retentions("iceberg", "2d", live_streams=True) == {
        "expire": "48h",
        "orphan": "48h",
    }
    assert applied_retentions("delta", "30m", live_streams=True) == {
        "expire": "168h",
        "orphan": "168h",
    }
    assert applied_retentions("delta", "30m", live_streams=False)["expire"] == "30m"


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
