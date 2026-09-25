"""Derived-name length limits refused at config load (LB-153)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import load_config
from lakebench.config.loader import ConfigValidationError
from lakebench.config.schema import (
    LakebenchConfig,
    derived_name_violations,
    max_namespace_length,
)


def _cfg(name: str, recipe: str | None = None, **platform) -> LakebenchConfig:
    data: dict = {"name": name}
    if recipe:
        data["recipe"] = recipe
    if platform:
        data["platform"] = platform
    return LakebenchConfig.model_validate(data)


def _exact(n: int) -> str:
    return "a" * n


class TestHiveLimit:
    def test_live_failure_name_is_refused(self):
        # The name that hung twice on the live cluster: 27 characters.
        with pytest.raises(ValidationError) as e:
            _cfg("ov-perf-c360-continuous-s10", "hive-iceberg-spark-trino")
        msg = str(e.value)
        assert "Hive metastore pod volume" in msg
        assert "lakebench-s3-credentials-ov-perf-c360-continuous-s10-s3-credentials" in msg
        assert "at most 23 characters" in msg

    def test_live_working_names_load(self):
        _cfg("ov-perf-c360-batch-s10", "hive-iceberg-spark-trino")
        _cfg("ov-perf-c360-cont", "hive-iceberg-spark-trino")

    def test_boundary_23_passes_24_fails(self):
        _cfg(_exact(23), "hive-iceberg-spark-trino")
        with pytest.raises(ValidationError, match="it is 24"):
            _cfg(_exact(24), "hive-iceberg-spark-trino")

    def test_default_catalog_is_hive(self):
        with pytest.raises(ValidationError):
            _cfg(_exact(24))

    def test_explicit_namespace_is_what_counts(self):
        # A long name is fine when the namespace is set short, and vice versa.
        _cfg(_exact(40), "hive-iceberg-spark-trino", kubernetes={"namespace": "short"})
        with pytest.raises(ValidationError, match="platform.kubernetes.namespace"):
            _cfg("short", "hive-iceberg-spark-trino", kubernetes={"namespace": _exact(24)})

    def test_ca_cert_tightens_nothing_below_23(self):
        # The CA volume (29 + ns) is looser than the credentials volume.
        cfg = _cfg(_exact(23), "hive-iceberg-spark-trino", storage={"s3": {"ca_cert": "/x.pem"}})
        assert max_namespace_length(cfg) == 23


class TestNonHiveLimit:
    @pytest.mark.parametrize("recipe", ["polaris-iceberg-spark-trino", "hive-iceberg-spark-trino"])
    def test_every_recipe_has_a_limit(self, recipe):
        cfg = _cfg("ok", recipe)
        assert max_namespace_length(cfg) <= 38

    def test_polaris_boundary_38_passes_39_fails(self):
        # Secret label secrets.stackable.tech/class = lakebench-s3-credentials-<ns>.
        _cfg(_exact(38), "polaris-iceberg-spark-trino")
        with pytest.raises(ValidationError, match="label secrets.stackable.tech/class"):
            _cfg(_exact(39), "polaris-iceberg-spark-trino")

    def test_name_over_63_refused(self):
        with pytest.raises(ValidationError):
            _cfg(_exact(64), "polaris-iceberg-spark-trino")

    def test_namespace_over_63_named(self):
        cfg = _cfg("ok", "polaris-iceberg-spark-trino")
        cfg.platform.kubernetes.namespace = _exact(64)
        msgs = derived_name_violations(cfg)
        assert any("Kubernetes allows at most 63" in m for m in msgs)


class TestBuckets:
    def test_bucket_boundary(self):
        ok = {"s3": {"buckets": {"bronze": "b" * 63, "silver": "abc", "gold": "gold"}}}
        _cfg("ok", "polaris-iceberg-spark-trino", storage=ok)
        bad = {"s3": {"buckets": {"bronze": "b" * 64}}}
        with pytest.raises(ValidationError, match="buckets.bronze"):
            _cfg("ok", "polaris-iceberg-spark-trino", storage=bad)


class TestLoader:
    def _write(self, tmp_path: Path, name: str) -> Path:
        p = tmp_path / "c.yaml"
        p.write_text(yaml.safe_dump({"name": name, "recipe": "hive-iceberg-spark-trino"}))
        return p

    def test_load_config_refuses(self, tmp_path):
        with pytest.raises(ConfigValidationError, match="at most 23"):
            load_config(self._write(tmp_path, "ov-perf-c360-continuous-s10"))

    def test_destroy_path_can_still_load(self, tmp_path):
        cfg = load_config(
            self._write(tmp_path, "ov-perf-c360-continuous-s10"), allow_long_names=True
        )
        assert cfg.get_namespace() == "ov-perf-c360-continuous-s10"

    @pytest.mark.parametrize(
        ("module", "argv"),
        [
            ("lakebench.cli._destroy", ["destroy"]),
            ("lakebench.cli._clean", ["clean", "data"]),
            ("lakebench.cli", ["status"]),
            ("lakebench.cli", ["stop"]),
            ("lakebench.cli", ["logs", "hive"]),
            ("lakebench.cli._admin", ["admin", "doctor"]),
            ("lakebench.cli._admin", ["admin", "reclaim-bucket"]),
            ("lakebench.cli._admin", ["admin", "repair-operator"]),
        ],
    )
    def test_teardown_and_recovery_commands_skip_the_check(
        self, module, argv, tmp_path, monkeypatch
    ):
        # A deployment that failed LB-153 still has buckets and secrets, so
        # the commands that clean up or inspect it must load its config.
        import importlib

        from typer.testing import CliRunner

        from lakebench.cli import app
        from lakebench.config.loader import ConfigFileNotFoundError

        monkeypatch.setenv("KUBECONFIG", "/nonexistent")
        seen: list[dict] = []

        def spy(path, **kwargs):
            seen.append(kwargs)
            raise ConfigFileNotFoundError(str(path))

        monkeypatch.setattr(importlib.import_module(module), "load_config", spy)
        cfg_path = self._write(tmp_path, "ov-perf-c360-continuous-s10")
        CliRunner().invoke(app, [*argv, str(cfg_path)])
        assert seen, f"{argv} never loaded the config"
        assert seen[0].get("allow_long_names") is True

    def test_deploy_path_keeps_the_check(self, tmp_path, monkeypatch):
        import importlib

        from typer.testing import CliRunner

        from lakebench.cli import app
        from lakebench.config.loader import ConfigFileNotFoundError

        monkeypatch.setenv("KUBECONFIG", "/nonexistent")
        seen: list[dict] = []

        def spy(path, **kwargs):
            seen.append(kwargs)
            raise ConfigFileNotFoundError(str(path))

        monkeypatch.setattr(importlib.import_module("lakebench.cli._deploy"), "load_config", spy)
        CliRunner().invoke(app, ["deploy", str(self._write(tmp_path, "x"))])
        assert seen and not seen[0].get("allow_long_names")


class TestPinnedPerfConfigs:
    @pytest.mark.parametrize(
        "path", sorted(Path(__file__).parent.parent.glob("benchmarks/perf/*-s*.yaml"))
    )
    def test_gate_loads_with_a_long_perf_name_in_the_env(self, path, monkeypatch):
        # The fingerprint does not depend on the name, so the gate must not
        # refuse because a too-long LAKEBENCH_PERF_NAME is still exported.
        from lakebench.metrics.perf_gate import load_pinned

        monkeypatch.setenv("LAKEBENCH_PERF_NAME", "ov-perf-c360-continuous-s10")
        load_pinned(path)

    @pytest.mark.parametrize(
        "path", sorted(Path(__file__).parent.parent.glob("benchmarks/perf/*-s*.yaml"))
    )
    def test_default_names_fit(self, path, monkeypatch):
        from lakebench.metrics.perf_gate import _PLACEHOLDER_ENV

        monkeypatch.delenv("LAKEBENCH_PERF_NAME", raising=False)
        for k, v in _PLACEHOLDER_ENV.items():
            if k != "LAKEBENCH_PERF_NAME":
                monkeypatch.setenv(k, v)
        cfg = load_config(path)
        assert len(cfg.get_namespace()) <= max_namespace_length(cfg)
