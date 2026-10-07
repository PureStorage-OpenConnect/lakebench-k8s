"""Derived-name length limits refused at config load (LB-153)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import load_config
from lakebench.config.loader import LoadPurpose
from lakebench.config.schema import (
    LakebenchConfig,
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

    def test_destroy_path_can_still_load(self, tmp_path):
        cfg = load_config(
            self._write(tmp_path, "ov-perf-c360-continuous-s10"), allow_long_names=True
        )
        assert cfg.get_namespace() == "ov-perf-c360-continuous-s10"

    @pytest.mark.parametrize(
        ("module", "argv"),
        [
            ("lakebench.cli._destroy", ["destroy"]),
            ("lakebench.cli._clean", ["clean", "silver"]),
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
        # TEARDOWN and READ skip the check; clean and stop keep MUTATE and
        # pass allow_long_names (config/loader.py load_config).
        kwargs = seen[0]
        assert kwargs.get("allow_long_names") is True or kwargs.get("purpose") in (
            LoadPurpose.TEARDOWN,
            LoadPurpose.READ,
        )
