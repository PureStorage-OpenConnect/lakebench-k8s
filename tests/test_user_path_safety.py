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

    @pytest.mark.parametrize("can_edit", [True, None])
    def test_pre_deploy_passes_when_the_add_is_possible_or_unknown(self, can_edit):
        level, _msg, _hint = operator_watch_verdict(
            "lb16-base", ["default"], namespace_exists=False, can_edit_release=can_edit
        )
        assert level == "ok"

    @pytest.mark.parametrize("exists", [True, False, None])
    def test_fails_when_credentials_cannot_make_the_add(self, exists):
        """Review: a namespace-scoped developer saw validate green, then
        deploy built half the stack and stopped at the operator step."""
        level, _msg, hint = operator_watch_verdict(
            "lb16-base",
            ["default"],
            namespace_exists=exists,
            can_edit_release=False,
            operator_namespace="spark-operator",
        )
        assert level == "fail"
        assert "spark-operator" in hint

    @pytest.mark.parametrize("can_edit", [True, False, None])
    @pytest.mark.parametrize("exists", [True, False, None])
    def test_never_suggests_raw_helm(self, exists, can_edit):
        _level, msg, hint = operator_watch_verdict(
            "lb16-base", ["default", "other"], namespace_exists=exists, can_edit_release=can_edit
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


@pytest.mark.parametrize(
    "example", ["hive-iceberg-spark-trino.yaml", "hive-delta-spark-thrift.yaml"]
)
def test_config_show_reports_peak_request(monkeypatch, example):
    """`config show` is the non-deprecated place for sizing; it must carry
    the same peak figure as info and the run preflight."""
    import pathlib

    from typer.testing import CliRunner

    from lakebench.cli import app, info_peak_request
    from lakebench.config import load_config

    for var in ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY"):
        monkeypatch.setenv(var, "placeholder")
    path = pathlib.Path(__file__).resolve().parents[1] / "examples" / example
    cfg = load_config(path)
    from lakebench.config.autosizer import resolve_auto_sizing

    resolve_auto_sizing(cfg)  # as info and run do
    peak, co_cores, co_gb, _ = info_peak_request(
        cfg, cfg.architecture.workload.datagen.scale, False
    )
    result = CliRunner().invoke(app, ["config", "show", str(path)], env={"COLUMNS": "200"})
    assert result.exit_code == 0, result.output
    assert (
        f"{peak.cpu_cores + co_cores} cores / {peak.memory_gb + co_gb} GB memory" in result.output
    )


@pytest.mark.parametrize("mode", ["batch", "sustained"])
def test_recommend_never_below_compute_peak_requirements(mode):
    """`recommend --scale` (and `config recommend`) sized Spark from the
    advisory compute_guidance(): 8 cores at scale 1 against 36 requested."""
    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    peak = compute_peak_requirements(1, mode)
    result = CliRunner().invoke(
        app, ["recommend", "--scale", "1", "--mode", mode], env={"COLUMNS": "200"}
    )
    assert result.exit_code == 0, result.output
    import re

    cores = int(re.search(r"CPU cores:\s+([\d,]+)", result.output).group(1).replace(",", ""))
    mem = int(re.search(r"Memory:\s+([\d,]+) GB", result.output).group(1).replace(",", ""))
    assert cores >= peak.cpu_cores
    assert mem >= peak.memory_gb


class TestWatchListEditsKeepTheInstalledChart:
    """A namespace add/remove is not an operator upgrade. Without --version
    Helm resolved the repo's latest chart (run path, install=false); with the
    config's version a developer could downgrade an admin's install."""

    def _mgr(self, target=None, installed=None):
        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        m = SparkOperatorManager(version=target, job_namespace="lb16")
        m._installed_version = installed
        return m

    def test_installed_chart_wins_over_config_and_latest(self):
        assert self._mgr(target="2.4.0", installed="2.5.1")._watch_list_pin()[:2] == [
            "--version",
            "2.5.1",
        ]
        assert self._mgr(target=None, installed="2.5.1")._watch_list_pin()[:2] == [
            "--version",
            "2.5.1",
        ]
        assert self._mgr(target="2.4.0")._watch_list_pin()[:2] == ["--version", "2.4.0"]
        assert self._mgr()._watch_list_pin() == []

    def test_add_pins_the_installed_chart(self, monkeypatch):
        from unittest.mock import MagicMock

        m = self._mgr(target=None, installed="2.5.1")
        calls = []

        def run(cmd, **_kw):
            calls.append(cmd)
            return MagicMock(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(m, "_run", run)
        monkeypatch.setattr(m, "_get_watched_namespaces", lambda: ["default"])
        monkeypatch.setattr(m, "_namespace_is_terminating", lambda _ns: False)
        monkeypatch.setattr(m, "_filter_existing_namespaces", lambda ns: ns)
        monkeypatch.setattr(m, "_restart_operator", lambda: True)
        monkeypatch.setattr(m, "_verify_namespace_watched", lambda *_a, **_k: True)
        m._add_namespace_to_watch_impl("lb16", _retry_on_eviction=False)
        upgrade = next(c for c in calls if c[:2] == ["helm", "upgrade"])
        assert upgrade[upgrade.index("--version") + 1] == "2.5.1"

    def test_recreate_rbac_refuses_without_the_lease(self, monkeypatch):
        from unittest.mock import MagicMock

        m = self._mgr(installed="2.5.1")
        run = MagicMock()
        monkeypatch.setattr(m, "_run", run)
        monkeypatch.setattr(m, "_acquire_watch_lease", lambda: (None, "refuse"))
        monkeypatch.setattr(m, "_get_watched_namespaces", lambda: ["default", "lb16"])
        assert m.recreate_namespace_rbac("lb16") is False
        run.assert_not_called()


def test_recommend_sizes_the_financial_schema():
    """Review: config recommend passed only the mode, so AML continuous was
    sized as Customer360 (38 cores against 118 requested at scale 1)."""
    import re

    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    peak = compute_peak_requirements(1, "sustained", "financial")
    out = (
        CliRunner()
        .invoke(
            app,
            ["recommend", "--scale", "1", "--mode", "sustained", "--schema", "financial"],
            env={"COLUMNS": "200"},
        )
        .output
    )
    cores = int(re.search(r"CPU cores:\s+([\d,]+)", out).group(1).replace(",", ""))
    assert cores >= peak.cpu_cores


@pytest.mark.parametrize("recorded", [True, False])
def test_continuous_reset_needs_the_record_on_tagless_backends(monkeypatch, recorded):
    """Review: the continuous reset deleted checkpoint and raw prefixes in any
    name-matching bucket on FlashBlade, record or not."""
    from unittest.mock import MagicMock, patch

    from lakebench.cli import _sustained
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    cfg = MagicMock()
    cfg.name = "a"
    cfg.get_namespace.return_value = "a"
    b = cfg.platform.storage.s3.buckets
    b.bronze, b.silver, b.gold = "a-bronze", "a-silver", "a-gold"
    with (
        patch("lakebench.s3.S3Client"),
        patch(
            "lakebench.deploy.ownership.verify_bucket_ownership",
            side_effect=lambda _c, bucket, _n: IdentityReport(
                verdict=IdentityVerdict.UNSUPPORTED, resource_name=bucket, expected_deployment="a"
            ),
        ),
        patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=[]),
        patch("lakebench.deploy.ownership.tagless_contents_are_ours", return_value=recorded),
    ):
        problem = _sustained._bucket_ownership_problem(cfg, MagicMock())
    assert (problem is None) is recorded


class TestInstalledChartLookup:
    """Review: `helm list -A -f spark-operator` took releases[0], so a
    look-alike release elsewhere could set the watch-list --version pin."""

    def _mgr(self, stdout, rc=0):
        from unittest.mock import MagicMock

        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        m = SparkOperatorManager(namespace="spark-operator")
        m._run = MagicMock(return_value=MagicMock(returncode=rc, stdout=stdout))
        return m

    def test_only_the_exact_release_in_its_namespace_counts(self):
        m = self._mgr(
            '[{"name":"my-spark-operator","chart":"spark-operator-9.9.9"},'
            '{"name":"spark-operator","chart":"spark-operator-2.5.1"}]'
        )
        assert m._get_helm_version() == "2.5.1"
        cmd = m._run.call_args.args[0]
        assert cmd[cmd.index("-n") + 1] == "spark-operator"
        assert "-A" not in cmd
        assert cmd[cmd.index("-f") + 1] == "^spark-operator$"

    def test_unparseable_chart_gives_no_pin(self):
        assert (
            self._mgr('[{"name":"spark-operator","chart":"mirror-chart"}]')._get_helm_version()
            is None
        )
