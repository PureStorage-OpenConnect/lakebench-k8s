"""User-path safety: validate before deploy, info sizing, remedy hints.

Each test names the defect it guards (lb16-base live report, outcome 5).
"""

from __future__ import annotations

import pytest

from lakebench.cli import operator_watch_verdict
from lakebench.modules.pipeline_engines.spark.operator import watch_list_fix_hint


class TestValidateWatchListVerdict:
    """`validate` failed on every example config before deploy because the
    namespace was not yet in spark.jobNamespaces, although deploy adds it."""

    def test_pre_deploy_is_not_a_failure(self):
        level, msg, hint = operator_watch_verdict("lb16-base", ["default"], namespace_exists=False)
        assert level == "ok"
        assert "deploy adds it" in msg

    @pytest.mark.parametrize("exists", [True, None])
    def test_existing_or_unknown_namespace_is_a_warning(self, exists):
        level, _msg, hint = operator_watch_verdict(
            "lb16-base", ["default"], namespace_exists=exists
        )
        assert level == "warn"
        assert "lakebench deploy" in hint

    @pytest.mark.parametrize("exists", [True, False, None])
    def test_never_suggests_raw_helm(self, exists):
        _level, msg, hint = operator_watch_verdict(
            "lb16-base", ["default", "other"], namespace_exists=exists
        )
        text = msg + hint
        assert "--reuse-values" not in text
        assert "helm upgrade" not in text
        assert "jobNamespaces={" not in text


def test_fix_hint_routes_through_the_lease():
    hint = watch_list_fix_hint()
    assert "lakebench deploy" in hint
    assert "--reuse-values" not in hint
    assert "helm upgrade" not in hint


def test_no_raw_watch_list_helm_in_user_docs_or_cli_strings():
    """No user-facing text may tell the user to set spark.jobNamespaces by
    hand with helm; that bypasses the lakebench-cluster-lock lease."""
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = []
    # A namespace *list* literal is the hazardous form: it is a snapshot of
    # someone else's watch list. A first install with jobNamespaces=""
    # (watch all) is an admin bootstrap and stays documented.
    pattern = re.compile(r"spark\.jobNamespaces=\{")
    for path in [root / "README.md", *sorted((root / "docs").glob("*.md"))]:
        if pattern.search(path.read_text()):
            offenders.append(str(path.relative_to(root)))
    for path in (root / "src" / "lakebench" / "cli").glob("*.py"):
        text = path.read_text()
        if "--reuse-values" in text or pattern.search(text):
            offenders.append(str(path.relative_to(root)))
    assert offenders == []


class TestInfoPeakRequest:
    """`info` said "17 cores needed" at scale 1 while
    compute_peak_requirements() says 36 cores / 512 GB (gotcha 34)."""

    @pytest.fixture
    def trino_example(self, monkeypatch):
        import pathlib

        for var in (
            "LAKEBENCH_POLARIS_CLIENT_SECRET",
            "LAKEBENCH_S3_ACCESS_KEY",
            "LAKEBENCH_S3_SECRET_KEY",
        ):
            monkeypatch.setenv(var, "placeholder")
        root = pathlib.Path(__file__).resolve().parents[1]
        return root / "examples" / "hive-iceberg-spark-trino.yaml"

    def test_info_reports_compute_peak_requirements(self, trino_example, monkeypatch):
        from unittest.mock import MagicMock

        from typer.testing import CliRunner

        import lakebench.cli as cli_mod
        from lakebench.cli import app, info_peak_request
        from lakebench.config import load_config
        from lakebench.k8s.client import ClusterCapacity
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        cfg = load_config(trino_example)
        scale = cfg.architecture.workload.datagen.get_effective_scale()
        peak, co_cores, co_gb, _ = info_peak_request(cfg, scale, False)
        ref = compute_peak_requirements(scale, "batch", cfg.architecture.workload.schema_type.value)
        assert (peak.cpu_cores, peak.memory_gb) == (ref.cpu_cores, ref.memory_gb)
        assert peak.cpu_cores >= 36 and peak.memory_gb >= 512  # scale 1 silver-build

        k8s = MagicMock()
        k8s.get_cluster_capacity.return_value = ClusterCapacity(
            total_cpu_millicores=434_000,
            total_memory_bytes=4349 * 1024**3,
            node_count=10,
            largest_node_cpu_millicores=64_000,
            largest_node_memory_bytes=512 * 1024**3,
        )
        monkeypatch.setattr(cli_mod, "get_k8s_client", lambda *a, **k: k8s)
        result = CliRunner().invoke(app, ["info", str(trino_example)], env={"COLUMNS": "200"})
        assert result.exit_code == 0, result.output
        out = " ".join(result.output.split())
        needed = ref.cpu_cores + co_cores
        assert "Peak requested" in out
        assert f"{needed} cores / {ref.memory_gb + co_gb} GB memory" in out
        assert f"peak request {needed} cores" in out
        assert "17 needed" not in out


def test_config_show_reports_peak_request(monkeypatch):
    """`config show` is the non-deprecated place for sizing; it must carry
    the same peak figure as info and the deploy preflight."""
    import pathlib

    from typer.testing import CliRunner

    from lakebench.cli import app, info_peak_request
    from lakebench.config import load_config

    for var in ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY"):
        monkeypatch.setenv(var, "placeholder")
    path = (
        pathlib.Path(__file__).resolve().parents[1] / "examples" / "hive-iceberg-spark-trino.yaml"
    )
    cfg = load_config(path)
    peak, co_cores, co_gb, _ = info_peak_request(
        cfg, cfg.architecture.workload.datagen.scale, False
    )
    result = CliRunner().invoke(app, ["config", "show", str(path)], env={"COLUMNS": "200"})
    assert result.exit_code == 0, result.output
    assert (
        f"{peak.cpu_cores + co_cores} cores / {peak.memory_gb + co_gb} GB memory" in result.output
    )
