"""v1.6 config contract (design-contradictions #4, #7, #12, #13, #15, #18, #19,
LB-181, outcome 5).

Each test here fails with its fix reverted: AML on Delta and the Java/Iceberg
pairing used to load, `custom` used to run the Customer 360 queries, the
workload block lived only under architecture, `sustained` was the canonical
mode, dead fields loaded silently, a benchmark exception left the run
successful, bucket names were fixed and global, and destroy uninstalled the
shared observability stack.
"""

from __future__ import annotations

import ast
import re
import subprocess
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config import load_config
from lakebench.config.loader import ConfigValidationError, save_config
from lakebench.config.schema import (
    LakebenchConfig,
    PipelineMode,
    is_continuous_mode,
)
from tests.conftest import make_config

ROOT = Path(__file__).resolve().parents[1]


def _write(tmp_path, data) -> Path:
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


def _quiet(fn, *a, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        return fn(*a, **kw)


# -- #4: workload x format compatibility, Java/Iceberg pairing ---------------


def test_aml_on_delta_is_refused_naming_the_supported_format():
    with pytest.raises(ValueError) as exc:
        make_config(recipe="hive-delta-spark-trino", workload={"schema": "financial"})
    msg = str(exc.value)
    assert "financial (AML) workload supports table_format iceberg, not delta" in msg


def test_c360_on_delta_still_loads():
    cfg = make_config(recipe="hive-delta-spark-trino", workload={"schema": "customer360"})
    assert cfg.architecture.table_format.type.value == "delta"


def test_iceberg_111_on_java11_spark_image_is_refused_at_load():
    with pytest.raises(ValueError, match="requires Java 17"):
        make_config(
            images={"spark": "apache/spark:3.5.4-python3"},
            architecture={"table_format": {"iceberg": {"version": "1.11.0"}}},
        )


def test_unset_iceberg_version_on_java11_image_uses_a_java11_release():
    cfg = make_config(images={"spark": "apache/spark:3.5.4-python3"})
    assert cfg.architecture.table_format.iceberg.version == "1.10.1"


def test_iceberg_on_spark35_loads_with_java17_or_older_iceberg():
    make_config(images={"spark": "apache/spark:3.5.9-java17-python3"})
    make_config(
        images={"spark": "apache/spark:3.5.4-python3"},
        architecture={"table_format": {"iceberg": {"version": "1.10.1"}}},
    )


# -- #13: custom is refused ---------------------------------------------------


def test_custom_workload_is_refused(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        load_config(_write(tmp_path, {"name": "t", "workload": {"schema": "custom"}}))
    msg = str(exc.value)
    assert "'custom' is not supported" in msg
    assert "workload.schema" in msg and "architecture.workload" not in msg


# -- #12: top-level workload --------------------------------------------------


def test_top_level_workload_is_canonical(tmp_path):
    cfg = _quiet(
        load_config,
        _write(tmp_path, {"name": "t", "workload": {"schema": "financial"}}),
    )
    assert cfg.workload.schema_type.value == "financial"
    assert cfg.architecture.workload is cfg.workload
    # The financial table defaults still follow the workload.
    assert cfg.architecture.tables.silver == "silver.transactions"


def test_architecture_workload_warns_and_names_new_location(tmp_path):
    with pytest.warns(DeprecationWarning, match="top-level 'workload:' key"):
        cfg = load_config(
            _write(tmp_path, {"name": "t", "architecture": {"workload": {"schema": "financial"}}})
        )
    assert cfg.workload.schema_type.value == "financial"


def test_both_locations_that_agree_are_merged(tmp_path):
    data = {
        "name": "t",
        "workload": {"schema": "financial"},
        "architecture": {"workload": {"schema": "financial", "datagen": {"scale": 3}}},
    }
    with pytest.warns(DeprecationWarning):
        cfg = load_config(_write(tmp_path, data))
    assert cfg.workload.schema_type.value == "financial"
    assert cfg.workload.datagen.scale == 3


def test_both_locations_that_disagree_are_refused(tmp_path):
    data = {
        "name": "t",
        "workload": {"datagen": {"scale": 5}},
        "architecture": {"workload": {"datagen": {"scale": 3}}},
    }
    with pytest.raises(ConfigValidationError, match="workload.datagen.scale: 5 at the top level"):
        load_config(_write(tmp_path, data))


def test_flat_scale_lands_in_the_top_level_block(tmp_path):
    cfg = _quiet(
        load_config,
        _write(tmp_path, {"name": "t", "scale": 7, "workload": {"schema": "customer360"}}),
    )
    assert cfg.workload.datagen.scale == 7


def test_flat_scale_with_legacy_block_does_not_create_a_conflict(tmp_path):
    data = {"name": "t", "scale": 7, "architecture": {"workload": {"schema": "financial"}}}
    with pytest.warns(DeprecationWarning):
        cfg = load_config(_write(tmp_path, data))
    assert cfg.workload.datagen.scale == 7
    assert cfg.workload.schema_type.value == "financial"


def test_saved_config_reloads_without_deprecations(tmp_path):
    cfg = make_config(
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": "continuous"}},
    )
    path = tmp_path / "saved.yaml"
    save_config(cfg, path)
    data = yaml.safe_load(path.read_text())
    assert "workload" in data and "workload" not in data["architecture"]
    assert "continuous" in data["architecture"]["pipeline"]
    again = _quiet(load_config, path)
    assert again.workload.schema_type.value == "financial"
    assert again.architecture.pipeline.mode == PipelineMode.CONTINUOUS


# -- #18: continuous is canonical ---------------------------------------------


def test_pipeline_mode_continuous_is_canonical():
    assert PipelineMode.CONTINUOUS.value == "continuous"
    assert PipelineMode.SUSTAINED is PipelineMode.CONTINUOUS
    assert PipelineMode("sustained") is PipelineMode.CONTINUOUS
    assert [m.value for m in PipelineMode] == ["batch", "continuous"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("continuous", True),
        ("sustained", True),  # metrics files still record this
        (PipelineMode.CONTINUOUS, True),
        ("batch", False),
        (None, False),
    ],
)
def test_is_continuous_mode(value, expected):
    assert is_continuous_mode(value) is expected


def test_peak_requirements_read_either_spelling():
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    a = compute_peak_requirements(1, "continuous")
    b = compute_peak_requirements(1, "sustained")
    c = compute_peak_requirements(1, PipelineMode.CONTINUOUS)
    batch = compute_peak_requirements(1, "batch")
    assert a == b == c
    assert a != batch


def test_run_cli_shows_continuous_and_hides_sustained():
    import typer.main

    from lakebench.cli import app

    run_cmd = typer.main.get_command(app).commands["run"]  # type: ignore[attr-defined]
    opts = {name: p for p in run_cmd.params for name in getattr(p, "opts", [])}
    assert "--continuous" in opts and not opts["--continuous"].hidden
    assert "--sustained" in opts and opts["--sustained"].hidden


def test_old_metrics_with_sustained_mode_still_read_as_continuous(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    pb = SimpleNamespace(pipeline_mode="sustained")
    metrics = SimpleNamespace(pipeline_benchmark=pb, streaming=[])
    gen = ReportGenerator.__new__(ReportGenerator)
    assert gen._is_sustained(metrics) is True


# -- #19: dead fields warn ----------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"images": {"prometheus": "prom/prometheus:v3"}}, "'prometheus' (ImagesConfig)"),
        ({"images": {"grafana": "grafana/grafana:13"}}, "'grafana' (ImagesConfig)"),
        ({"observability": {"reports": {"format": "json"}}}, "'reports' (ObservabilityConfig)"),
        (
            {"architecture": {"table_format": {"iceberg": {"file_format": "orc"}}}},
            "'file_format' (IcebergConfig)",
        ),
        (
            {"architecture": {"table_format": {"iceberg": {"properties": {"a": 1}}}}},
            "'properties' (IcebergConfig)",
        ),
        (
            {
                "recipe": "hive-delta-spark-trino",
                "architecture": {"table_format": {"delta": {"properties": {"a": 1}}}},
            },
            "'properties' (DeltaConfig)",
        ),
    ],
)
def test_dead_field_set_warns_no_effect_and_v17_removal(overrides, field):
    with pytest.warns(DeprecationWarning) as rec:
        make_config(**overrides)
    msgs = [str(w.message) for w in rec]
    hit = [m for m in msgs if field in m]
    assert hit, msgs
    assert "has no effect" in hit[0] and "removed in v1.7" in hit[0]


def test_dead_fields_at_default_do_not_warn():
    _quiet(
        make_config,
        images={"prometheus": "prom/prometheus:v2.48.0"},
        observability={"reports": {"enabled": True}},
    )


def test_pipeline_pattern_other_than_medallion_warns():
    with pytest.warns(DeprecationWarning, match="'pipeline.pattern: streaming' is deprecated"):
        make_config(architecture={"pipeline": {"pattern": "streaming"}})


# -- LB-181: user-facing notes cite nothing internal --------------------------


def test_recipe_and_combination_notes_cite_no_internal_gotchas():
    from lakebench.config.recipes import RECIPE_NOTES
    from lakebench.config.schema import _COMBINATION_NOTES

    texts = list(_COMBINATION_NOTES.values())
    for note in RECIPE_NOTES.values():
        texts.append(note.when)
        texts.extend(note.caveats)
    bad = [t for t in texts if re.search(r"gotcha|CLAUDE\.md|\bLB-\d+", t)]
    assert not bad, bad


# -- Outcome 5: bucket names and the documented quick start -------------------


def test_unset_buckets_default_to_deployment_name():
    cfg = make_config(name="lab-a")
    b = cfg.platform.storage.s3.buckets
    assert (b.bronze, b.silver, b.gold) == ("lab-a-bronze", "lab-a-silver", "lab-a-gold")


def test_explicit_buckets_are_kept():
    cfg = make_config(
        name="lab-a",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "k",
                    "secret_key": "s",
                    "buckets": {"bronze": "shared-bronze"},
                }
            }
        },
    )
    b = cfg.platform.storage.s3.buckets
    assert (b.bronze, b.silver, b.gold) == ("shared-bronze", "lab-a-silver", "lab-a-gold")


def _cli_tree():
    import typer.main

    from lakebench.cli import app

    return typer.main.get_command(app)


def _resolve(group, words):
    """Return (command, remaining words) for a 'lakebench ...' line."""
    cmd = group
    rest = list(words)
    while hasattr(cmd, "commands") and rest and not rest[0].startswith("-"):
        nxt = cmd.commands.get(rest[0])  # type: ignore[attr-defined]
        if nxt is None:
            break
        cmd, rest = nxt, rest[1:]
    return cmd, rest


def _documented_commands(path: Path) -> list[str]:
    text = path.read_text()
    lines = []
    for block in re.findall(r"```bash\n(.*?)```", text, re.S):
        for line in block.splitlines():
            line = line.split("#", 1)[0].strip()
            if line.startswith("lakebench "):
                lines.append(line)
    return lines


@pytest.mark.parametrize("doc", ["README.md", "docs/getting-started.md"])
def test_documented_commands_are_valid_cli(doc):
    tree = _cli_tree()
    problems = []
    for line in _documented_commands(ROOT / doc):
        cmd, rest = _resolve(tree, line.split()[1:])
        if hasattr(cmd, "commands"):
            problems.append(f"{line}: not a command")
            continue
        known = {o for p in cmd.params for o in [*p.opts, *(p.secondary_opts or [])]}
        for word in rest:
            if word.startswith("-"):
                flag = word.split("=", 1)[0]
                if flag not in known:
                    problems.append(f"{line}: unknown option {flag}")
    assert not problems, problems


def test_readme_quick_start_run_can_deploy():
    # Without --yes, 'run' refuses when the namespace does not exist yet, so
    # the quick start (init -> run -> results -> destroy) would stop there.
    runs = [ln for ln in _documented_commands(ROOT / "README.md") if ln.split()[1:2] == ["run"]]
    first = runs[0]
    assert "--generate" in first and ("--yes" in first.split() or "-y" in first.split())


# -- #7: a benchmark exception fails the run ----------------------------------


def _benchmark_except_handler() -> ast.ExceptHandler:
    src = (ROOT / "src/lakebench/cli/_run.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and "Benchmark did not complete" in ast.unparse(
            node
        ):
            return node
    raise AssertionError("benchmark exception handler not found")


def test_benchmark_exception_fails_the_run_and_withholds_qph():
    body = ast.unparse(_benchmark_except_handler())
    assert "pipeline_success = False" in body
    assert "benchmark_qph = None" in body
    assert "collector.current_run.benchmark = None" in body
    assert "benchmark_error" in body
    assert "success=False" in body  # journal event


def test_benchmark_error_round_trips_and_reaches_the_report(tmp_path):
    from datetime import datetime

    from lakebench.metrics.collector import PipelineMetrics
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    run = PipelineMetrics(
        run_id="r1",
        deployment_name="d",
        start_time=datetime.now(),
        success=False,
        benchmark_error="RuntimeError: trino down",
    )
    assert run.to_dict()["benchmark_error"] == "RuntimeError: trino down"
    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    loaded = storage.load_run("r1")
    assert loaded.benchmark_error == "RuntimeError: trino down"
    gen = ReportGenerator.__new__(ReportGenerator)
    ok, reasons, _ = gen._compute_overall_status(loaded)
    assert not ok
    assert any("Benchmark did not complete" in r for r in reasons)


# -- #15: shared observability stack ------------------------------------------


def _obs_engine(namespace="dep-a", dry_run=False):
    cfg = make_config(
        name=namespace,
        observability={"enabled": True},
    )
    return SimpleNamespace(
        config=cfg,
        k8s=MagicMock(),
        renderer=MagicMock(),
        context={},
        dry_run=dry_run,
    )


def _helm_list_result(releases):
    import json

    return subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(releases))


def test_destroy_never_uninstalls_the_shared_stack():
    from lakebench.deploy.observability import ObservabilityDeployer

    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        # This deployment's namespace holds no release of its own.
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

    deployer = ObservabilityDeployer(_obs_engine())
    with patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run):
        result = deployer.destroy()
    assert not any("uninstall" in c for c in calls), calls
    assert "left in place" in result.message


def test_destroy_step_with_observability_enabled_makes_no_uninstall_call():
    """The destroy engine's observability step, end to end through the deployer."""
    import inspect

    from lakebench.deploy import destroy as destroy_mod

    src = inspect.getsource(destroy_mod)
    step = src[src.index("# Step 5: Observability") : src.index("# Step 6")]
    assert "ObservabilityDeployer(engine)" in step
    assert "helm" not in step.replace("kube-prometheus-stack", "")


def test_destroy_removes_only_a_legacy_release_in_its_own_namespace():
    from lakebench.deploy.observability import HELM_RELEASE_NAME, ObservabilityDeployer

    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        if cmd[:2] == ["helm", "list"]:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=HELM_RELEASE_NAME)
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    deployer = ObservabilityDeployer(_obs_engine("dep-a"))
    with patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run):
        deployer.destroy()
    uninstalls = [c for c in calls if "uninstall" in c]
    assert uninstalls == [["helm", "uninstall", HELM_RELEASE_NAME, "--namespace", "dep-a"]]


def test_destroy_in_the_shared_namespace_never_uninstalls():
    from lakebench.deploy.observability import OBSERVABILITY_NAMESPACE, ObservabilityDeployer

    # Config load refuses this namespace; the deployer guards it anyway.
    engine = _obs_engine()
    engine.config = MagicMock()
    engine.config.get_namespace.return_value = OBSERVABILITY_NAMESPACE
    deployer = ObservabilityDeployer(engine)
    with patch("lakebench.deploy.observability.subprocess.run") as run:
        deployer.destroy()
    assert run.call_count == 0


@pytest.fixture
def _lock():
    with (
        patch("kubernetes.client.CoreV1Api"),
        patch("lakebench.deploy.cluster_lock.cluster_lock") as lock,
        patch("lakebench.deploy.observability._wait_for_prometheus", return_value="") as wait,
    ):
        lock.wait = wait
        lock.return_value.__enter__.return_value = None
        lock.return_value.__exit__.return_value = False
        yield lock


def test_deploy_reuses_an_existing_release_without_modifying_it(_lock):
    from lakebench.deploy.engine import DeploymentStatus
    from lakebench.deploy.observability import (
        HELM_RELEASE_NAME,
        OBSERVABILITY_NAMESPACE,
        ObservabilityDeployer,
    )

    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return _helm_list_result(
            [
                {
                    "name": HELM_RELEASE_NAME,
                    "namespace": OBSERVABILITY_NAMESPACE,
                    "status": "deployed",
                }
            ]
        )

    deployer = ObservabilityDeployer(_obs_engine())
    with patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run):
        result = deployer.deploy()
    assert result.status == DeploymentStatus.SUCCESS
    assert all(c[:2] == ["helm", "list"] for c in calls), calls
    assert "shared cluster component" in result.message


def test_deploy_installs_into_the_shared_namespace_only_when_absent(_lock):
    from lakebench.deploy.observability import OBSERVABILITY_NAMESPACE, ObservabilityDeployer

    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        if cmd[:2] == ["helm", "list"]:
            return _helm_list_result([])
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    deployer = ObservabilityDeployer(_obs_engine())
    with (
        patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run),
        patch("lakebench.deploy.observability._find_helm_service", return_value=None),
        patch.object(ObservabilityDeployer, "_is_openshift", return_value=False),
    ):
        deployer.deploy()
    installs = [c for c in calls if c[:2] == ["helm", "install"]]
    assert len(installs) == 1
    assert installs[0][installs[0].index("--namespace") + 1] == OBSERVABILITY_NAMESPACE
    assert not [c for c in calls if c[:2] == ["helm", "upgrade"]]
    assert _lock.called


def test_deploy_does_not_install_when_the_lookup_fails(_lock):
    from lakebench.deploy.engine import DeploymentStatus
    from lakebench.deploy.observability import ObservabilityDeployer

    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="boom")

    deployer = ObservabilityDeployer(_obs_engine())
    with patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run):
        result = deployer.deploy()
    assert result.status == DeploymentStatus.FAILED
    assert not [c for c in calls if c[1] in ("install", "upgrade")]


def test_helm_values_do_not_pin_scraping_to_one_namespace():
    from lakebench.deploy.observability import ObservabilityDeployer

    values = ObservabilityDeployer(_obs_engine())._build_helm_values("dep-a")
    assert not any("NamespaceSelector" in k for k in values)


def test_example_configs_load_quietly():
    # Examples teach the canonical spellings (top-level workload, continuous).
    for path in sorted((ROOT / "examples").glob("*.yaml")):
        text = path.read_text()
        assert not re.search(r"(?m)^  workload:", text), path.name
        assert "mode: sustained" not in text, path.name


def test_config_template_uses_canonical_keys():
    from lakebench.config.loader import generate_example_config_yaml

    text = generate_example_config_yaml()
    assert re.search(r"(?m)^workload:", text)
    assert not re.search(r"(?m)^  workload:", text)
    assert "iot" not in text
    assert "sustained:" not in text
    parsed = yaml.safe_load(text)
    parsed["name"] = "tmpl"
    _quiet(LakebenchConfig.model_validate, parsed)


@pytest.mark.parametrize("name", ["lakebench-observability", "lakebench-system"])
def test_reserved_namespaces_are_refused(name):
    with pytest.raises(ValueError, match="reserved for shared lakebench state"):
        make_config(name=name)
    with pytest.raises(ValueError, match="reserved"):
        make_config(name="ok", platform={"kubernetes": {"namespace": name}})


def test_invalid_derived_bucket_name_is_refused():
    with pytest.raises(ValueError, match="not a valid S3 bucket name"):
        make_config(name="My_Deploy", platform={"kubernetes": {"namespace": "my-deploy"}})


def test_deploy_refuses_to_reuse_a_release_that_is_not_deployed(_lock):
    from lakebench.deploy.engine import DeploymentStatus
    from lakebench.deploy.observability import (
        HELM_RELEASE_NAME,
        OBSERVABILITY_NAMESPACE,
        ObservabilityDeployer,
    )

    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return _helm_list_result(
            [
                {
                    "name": HELM_RELEASE_NAME,
                    "namespace": OBSERVABILITY_NAMESPACE,
                    "status": "pending-install",
                }
            ]
        )

    deployer = ObservabilityDeployer(_obs_engine())
    with patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run):
        result = deployer.deploy()
    assert result.status == DeploymentStatus.FAILED
    assert "status 'pending-install'" in result.message
    assert all(c[:2] == ["helm", "list"] for c in calls), calls


def test_destroy_reports_failure_when_it_cannot_list_releases():
    from lakebench.deploy.engine import DeploymentStatus
    from lakebench.deploy.observability import ObservabilityDeployer

    def fake_run(cmd, **kw):
        return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="no helm")

    deployer = ObservabilityDeployer(_obs_engine())
    with patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run) as run:
        result = deployer.destroy()
    assert result.status == DeploymentStatus.FAILED
    assert not any("uninstall" in c.args[0] for c in run.call_args_list)


def test_benchmark_postprocessing_error_keeps_a_recorded_result():
    body = ast.unparse(_benchmark_except_handler())
    assert "if _bench_recorded:" in body


def test_errors_name_the_continuous_block_the_user_wrote(tmp_path):
    data = {"name": "t", "architecture": {"pipeline": {"continuous": {"run_duration": -5}}}}
    with pytest.raises(ConfigValidationError) as exc:
        load_config(_write(tmp_path, data))
    assert "architecture.pipeline.continuous.run_duration" in str(exc.value)


def test_install_waits_for_prometheus_after_the_lease_and_fails_if_not_ready(_lock):
    from lakebench.deploy.engine import DeploymentStatus
    from lakebench.deploy.observability import OBSERVABILITY_NAMESPACE, ObservabilityDeployer

    order = []
    _lock.return_value.__exit__.side_effect = lambda *a: order.append("lease released")
    _lock.wait.side_effect = lambda ns: order.append(f"wait {ns}") or "not Ready after 600s"

    def fake_run(cmd, **kw):
        if cmd[:2] == ["helm", "list"]:
            return _helm_list_result([])
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    deployer = ObservabilityDeployer(_obs_engine())
    with (
        patch("lakebench.deploy.observability.subprocess.run", side_effect=fake_run),
        patch("lakebench.deploy.observability._find_helm_service", return_value=None),
        patch.object(ObservabilityDeployer, "_is_openshift", return_value=False),
    ):
        result = deployer.deploy()
    assert order == ["lease released", f"wait {OBSERVABILITY_NAMESPACE}"]
    assert result.status == DeploymentStatus.FAILED
    assert "not Ready" in result.message


def test_recipe_config_on_java11_image_falls_back_to_a_java11_iceberg():
    cfg = make_config(
        recipe="hive-iceberg-spark-trino", images={"spark": "apache/spark:3.5.4-python3"}
    )
    assert cfg.architecture.table_format.iceberg.version == "1.10.1"
