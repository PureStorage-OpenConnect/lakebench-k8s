"""The consumers of the dependency set: Spark jobs, Spark Thrift, DuckDB
(DEP-2, ch01 s2.6).

Every Spark job names the set's jars by URL in the manifest's jar order and
resolves nothing at submit; Thrift and DuckDB fetch the same files and check
each against the manifest ConfigMap; nothing is built without a verified set.
"""

from __future__ import annotations

import itertools
import re
from unittest.mock import MagicMock, patch
from urllib.parse import unquote, urlsplit

import pytest
import yaml

from lakebench.config.recipes import RECIPES
from lakebench.deploy.engine import DeploymentEngine, TemplateRenderer
from lakebench.deps import manifest as m
from lakebench.modules.pipeline_engines.spark.job import (
    REFERENCE_SET_JOB_TYPES,
    JobType,
    SparkJobManager,
)
from lakebench.modules.pipeline_engines.spark.monitor import classify_dependency_failure
from tests.conftest import make_config

RECIPE_NAMES = sorted(r for r in RECIPES if r != "default")
OWNED = sorted(m.OWNED_SPARK_CONF_KEYS)


def _configs():
    for recipe, schema in itertools.product(RECIPE_NAMES, ("customer360", "financial")):
        try:
            cfg = make_config(recipe=recipe, workload={"schema": schema})
        except Exception:  # noqa: BLE001 -- an unsupported combination
            continue
        yield f"{recipe}-{schema}", cfg


CONFIGS = list(_configs())


def _manager(cfg, handle=None):
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(cfg, k8s)
    mgr.deps = handle or m.placeholder_handle(cfg)
    return mgr


def _job_types(cfg):
    for jt in JobType:
        if jt in REFERENCE_SET_JOB_TYPES and cfg.architecture.workload.schema_type.value != (
            "financial"
        ):
            continue
        yield jt


@pytest.mark.parametrize("name,cfg", CONFIGS, ids=[n for n, _ in CONFIGS])
def test_built_conf_has_no_packages(name, cfg):
    """For every supported row and job type: nothing resolved at submit,
    every jar from the config's own lb-deps set, in jar order, annotated."""
    mgr = _manager(cfg)
    handle = mgr.deps
    for jt in _job_types(cfg):
        spec = mgr._build_manifest(jt)["spec"]
        conf = spec["sparkConf"]
        for key in (
            "spark.jars.packages",
            "spark.jars.repositories",
            "spark.jars.ivy",
            "spark.jars.ivySettings",
        ):
            assert key not in conf, (jt, key)
        urls = conf["spark.jars"].split(",")
        assert all(u.startswith(handle.base_url + "/jars/") for u in urls), jt
        assert [unquote(u.rsplit("/", 1)[1]) for u in urls] == list(handle.manifest["jar_order"])
        driver = spec["driver"]["template"]["spec"]
        executor = spec["executor"]["template"]["spec"]
        for tpl in (driver, executor):
            names = {v["name"] for v in tpl["volumes"]}
            assert "spark-ivy-cache" not in names
            inits = {c["name"] for c in tpl.get("initContainers", [])}
            assert "resolve-deps" not in inits and "install-pydeps" not in inits
        assert "lb-deps-dl" not in {v["name"] for v in executor["volumes"]}
        for side in ("driver", "executor"):
            assert spec[side]["annotations"][m.POD_ANNOTATION_SET] == handle.pinset_sha256
        if cfg.architecture.table_format.type.value == "delta":
            assert conf["spark.submit.pyFiles"].endswith(
                "/jars/" + m.delta_jar(handle).replace("+", "%2B")
            )
        else:
            assert "spark.submit.pyFiles" not in conf


def test_jar_order_is_the_manifest_order():
    cfg = make_config(recipe="hive-iceberg-spark-trino")
    h = m.placeholder_handle(cfg)
    flipped = m.DepsHandle(
        h.pinset_sha256,
        h.request_sha256,
        h.base_url,
        "",
        {**h.manifest, "jar_order": list(reversed(h.manifest["jar_order"]))},
    )
    conf = _manager(cfg, flipped)._build_manifest(JobType.SILVER_BUILD)["spec"]["sparkConf"]
    names = [unquote(u.rsplit("/", 1)[1]) for u in conf["spark.jars"].split(",")]
    assert names == list(reversed(h.manifest["jar_order"]))


@pytest.mark.parametrize("key", OWNED)
def test_lakebench_owned_spark_conf_keys_are_refused(key):
    """At config load (exit 2, before anything is recorded), and again when
    a manifest is built from a config mutated after load."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match=re.escape(key)):
        make_config(spark={"conf": {key: "x"}})
    cfg = make_config()
    cfg.spark.conf[key] = "x"
    with pytest.raises(ValueError, match=re.escape(key)):
        _manager(cfg)._build_manifest(JobType.BRONZE_VERIFY)


@pytest.mark.real_deps
def test_a_refused_manifest_keeps_the_previous_application():
    """The manifest is built before the previous application is deleted."""
    cfg = make_config()
    cfg.spark.conf["spark.jars"] = "x"
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(cfg, k8s)
    mgr.deps = m.placeholder_handle(cfg)
    mgr._delete_job = MagicMock()
    with pytest.raises(ValueError):
        mgr.submit_job(JobType.BRONZE_VERIFY)
    mgr._delete_job.assert_not_called()


@pytest.mark.real_deps
def test_nothing_is_built_or_submitted_without_a_set():
    cfg = make_config()
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(cfg, k8s)
    assert mgr.deps is None
    with pytest.raises(m.DepsSetMissing):
        mgr._build_manifest(JobType.BRONZE_VERIFY)
    mgr._delete_job = MagicMock()
    with pytest.raises(m.DepsSetMissing):
        mgr.submit_job(JobType.BRONZE_VERIFY)
    mgr._delete_job.assert_not_called()  # the previous application survives


def test_the_reference_job_needs_the_reference_wheels():
    cfg = make_config(workload={"schema": "financial"})
    h = m.placeholder_handle(cfg)
    no_wheels = m.DepsHandle(
        h.pinset_sha256,
        h.request_sha256,
        h.base_url,
        "",
        {**h.manifest, "groups": {"jars": h.manifest["groups"]["jars"]}},
    )
    with pytest.raises(m.DepsSetMissing, match="py-reference"):
        _manager(cfg, no_wheels)._build_manifest(JobType.SCORE_FINANCIAL_REFERENCE)


def test_reference_wheels_install_from_the_set_only():
    cfg = make_config(workload={"schema": "financial"})
    mgr = _manager(cfg)
    drv = mgr._build_manifest(JobType.SCORE_FINANCIAL_REFERENCE)["spec"]["driver"]
    (init,) = [
        c for c in drv["template"]["spec"]["initContainers"] if c["name"] == "lb-deps-py-reference"
    ]
    cmd = init["command"][-1]
    host = urlsplit(mgr.deps.base_url).hostname
    assert f"--trusted-host {host} " in cmd
    assert "--no-index" in cmd and "--require-hashes" in cmd
    # Retried from an empty target: a server restart does not fail a look.
    assert cmd.startswith("for i in") and "rm -rf /opt/lb-pydeps/*" in cmd


@pytest.mark.real_deps
def test_the_fingerprint_builds_offline_without_a_deployment():
    """The perf fingerprint builds manifests offline: production code gives
    it the placeholder set (no conftest help here)."""
    from lakebench.metrics.fingerprint_inputs import _build

    out = _build(make_config(), continuous=False)
    for conf in out["owned_conf"].values():
        assert "spark.jars" not in conf and "spark.jars.packages" not in conf


# --- the monitor ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "log,needle",
    [
        (
            'Exception in thread "main" java.io.FileNotFoundException: '
            "http://lb-deps.ns.svc.cluster.local:8080/sets/abc/jars/x.jar\n"
            "\tat sun.net.www.protocol.http.HttpURLConnection.getInputStream",
            "does not serve this set",
        ),
        (
            "java.io.IOException: Server returned HTTP response code: 503 for URL: "
            "http://lb-deps.ns.svc.cluster.local:8080/sets/abc/jars/x.jar",
            "server error 503",
        ),
        (
            "java.net.ConnectException: Connection refused\n"
            "\tat org.apache.spark.util.Utils$.doFetchFile(Utils.scala:600)",
            "unreachable",
        ),
        (
            "java.net.UnknownHostException: lb-deps.ns.svc.cluster.local\n"
            "\tat org.apache.spark.util.DependencyUtils$.downloadFile",
            "unreachable",
        ),
    ],
)
def test_driver_fetch_failures_are_named(log, needle):
    assert needle in classify_dependency_failure(log)


def test_unrelated_failures_are_not_blamed_on_the_set():
    assert classify_dependency_failure("java.net.ConnectException: Connection refused (s3)") is None
    assert classify_dependency_failure(None) is None


# --- Spark Thrift and DuckDB -----------------------------------------------------------


def _render(cfg, template):
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
    handle = m.placeholder_handle(cfg)
    ctx = {**engine.context, **m.consumer_context(handle)}
    docs = [d for d in yaml.safe_load_all(TemplateRenderer().render(template, ctx)) if d]
    return next(d for d in docs if d["kind"] == "Deployment"), handle


@pytest.mark.parametrize("recipe", ["hive-iceberg-spark-thrift", "hive-delta-spark-thrift"])
def test_thrift_fetches_the_set_and_resolves_nothing(recipe):
    cfg = make_config(recipe=recipe)
    dep, handle = _render(cfg, "spark-thrift/sparkapplication.yaml.j2")
    assert dep["spec"]["strategy"]["type"] == "Recreate"
    tpl = dep["spec"]["template"]
    assert tpl["metadata"]["annotations"][m.POD_ANNOTATION_SET] == handle.pinset_sha256
    (fetch,) = [c for c in tpl["spec"]["initContainers"] if c["name"] == "lb-deps-fetch"]
    assert fetch["command"][:4] == ["python3", f"{m.TOOLS_MOUNT}/lb_deps.py", "fetch", "--group"]
    assert fetch["command"][fetch["command"].index("--url") + 1] == handle.base_url
    vols = {v["name"]: v for v in tpl["spec"]["volumes"]}
    assert vols["lb-deps-tools"]["configMap"]["name"] == m.tools_configmap_name(
        handle.request_sha256
    )
    assert vols["lb-deps-manifest"]["configMap"]["name"] == m.MANIFEST_CONFIGMAP
    main = tpl["spec"]["containers"][0]["command"][-1]
    assert (
        "--packages" not in main and "spark.jars.ivy" not in main and "cp /extra-jars" not in main
    )
    text = yaml.safe_dump(dep)
    assert "--packages" not in text and "download-jars" not in text


def test_duckdb_installs_from_the_set_with_hashes():
    cfg = make_config(recipe="hive-iceberg-spark-duckdb")
    dep, handle = _render(cfg, "duckdb/deployment.yaml.j2")
    assert dep["spec"]["strategy"]["type"] == "Recreate"
    tpl = dep["spec"]["template"]
    assert tpl["metadata"]["annotations"][m.POD_ANNOTATION_SET] == handle.pinset_sha256
    (init,) = tpl["spec"]["initContainers"]
    script = init["command"][-1]
    host = urlsplit(handle.base_url).hostname
    # fetch first: it refuses a manifest whose pinset is not the URL's.
    assert script.index("lb_deps.py fetch --group duckdb-ext") < script.index("pip install")
    assert f"--trusted-host {host}" in script and "--require-hashes" in script
    assert "--no-index" in script
    main = tpl["spec"]["containers"][0]
    assert main["command"] == ["sleep", "infinity"]
    assert {"name": "PYTHONPATH", "value": "/opt/lb-deps/py"} in main["env"]
    probe = " ".join(main["startupProbe"]["exec"]["command"])
    assert "autoinstall_known_extensions=false" in probe
    assert "INSTALL" not in yaml.safe_dump(dep)


def test_the_duckdb_executor_never_autoinstalls():
    from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor

    ex = DuckDBExecutor.__new__(DuckDBExecutor)
    ex.table_format = "iceberg"
    ex.s3_endpoint, ex.s3_region, ex.s3_path_style = "http://s3:80", "us-east-1", True
    script = ex._build_python_script("SELECT 1")
    assert script.index("autoinstall_known_extensions=false") < script.index("load_extension")


@pytest.mark.parametrize("recipe", ["hive-iceberg-spark-thrift", "hive-iceberg-spark-duckdb"])
def test_consumers_refuse_a_real_deploy_without_a_set(recipe):
    from lakebench.deploy.duckdb import DuckDBDeployer
    from lakebench.deploy.engine import DeploymentStatus
    from lakebench.modules.query_engines.spark_thrift.deployer import SparkThriftDeployer

    cfg = make_config(recipe=recipe)
    engine = MagicMock(config=cfg, dry_run=False, deps=None, context={})
    deployer = (DuckDBDeployer if "duckdb" in recipe else SparkThriftDeployer)(engine)
    result = deployer.deploy()
    assert result.status == DeploymentStatus.FAILED
    assert "no verified dependency set" in result.message
    engine.k8s.apply_manifest.assert_not_called()


def _consumer_cluster(rec, *, exit_code: int, log: str, phase: str = "Pending"):
    from kubernetes.client.models import V1DeploymentStatus

    rec.configure(namespace="ns")
    rec.add_namespace("ns")
    labels = {"app.kubernetes.io/component": "duckdb"}
    rec.add(
        "deployments",
        {
            "metadata": {"name": "lakebench-duckdb", "generation": 1},
            "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {}},
        },
        namespace="ns",
    )
    rec.store[("deployments", "ns", "lakebench-duckdb")].status = V1DeploymentStatus(
        observed_generation=1, replicas=1, updated_replicas=1, ready_replicas=0
    )
    pod = {
        "metadata": {
            "name": "d-1",
            "labels": labels,
            "annotations": {m.POD_ANNOTATION_SET: "p" * 64},
        },
        "status": {
            "phase": phase,
            "initContainerStatuses": [
                {
                    "name": "lb-deps-fetch",
                    "ready": False,
                    "restartCount": 0,
                    "image": "python:3.11-slim",
                    "imageID": "",
                    "state": {"terminated": {"exitCode": exit_code}},
                }
            ],
        },
    }
    rec.pod_logs[("ns", "d-1")] = log
    return pod


def test_a_stale_failed_consumer_pod_is_replaced_before_the_wait(recording_k8s):
    """A re-run after a failed fetch deletes the failing pod on this set, so
    kubelet's back-off on it does not fail or stall the new deploy."""
    from lakebench.deploy.deps import wait_consumer_rolled
    from tests.fixtures.recording_k8s import OWN

    rec = recording_k8s
    pod = _consumer_cluster(rec, exit_code=3, log="LB_DEPS_ERROR cannot fetch x\n")
    rec.add("pods", pod, namespace="ns")
    with (
        patch("lakebench.k8s.wait.time.sleep"),
        pytest.raises(RuntimeError, match="0/0 on the set"),
    ):
        wait_consumer_rolled("ns", "lakebench-duckdb", "p" * 64, timeout_seconds=1, what="DuckDB")
    rec.assert_recorded(verb="delete", kind="pods", name="d-1", scope=OWN)


@pytest.mark.parametrize("phase", ["Failed", "Succeeded"])
def test_a_finished_consumer_pod_is_not_counted(recording_k8s, phase):
    from lakebench.deps import runtime

    rec = recording_k8s
    pod = _consumer_cluster(rec, exit_code=0, log="", phase=phase)
    pod["metadata"]["annotations"][m.POD_ANNOTATION_SET] = "old" * 21 + "x"
    rec.add("pods", pod, namespace="ns")
    cfg = make_config(recipe="hive-iceberg-spark-duckdb", platform={"kubernetes": {"namespace": "ns"},
        "storage": {"s3": {"endpoint": "http://m:9000", "access_key": "k", "secret_key": "s"}}})  # fmt: skip
    from kubernetes import client as k8s_client

    assert runtime._consumer_mismatches(k8s_client.CoreV1Api(), "ns", cfg, "p" * 64) == []
    assert runtime.pods_on_sets(k8s_client.CoreV1Api(), "ns", cfg) == []


def test_a_consumer_fetch_failure_fails_the_wait_at_once(recording_k8s):
    from kubernetes.client.models import V1DeploymentStatus

    from lakebench.deploy.deps import wait_consumer_rolled

    rec = recording_k8s
    rec.configure(namespace="ns")
    rec.add_namespace("ns")
    labels = {"app.kubernetes.io/component": "duckdb"}
    rec.add(
        "deployments",
        {
            "metadata": {"name": "lakebench-duckdb", "generation": 1},
            "spec": {"replicas": 1, "selector": {"matchLabels": labels}, "template": {}},
        },
        namespace="ns",
    )
    rec.store[("deployments", "ns", "lakebench-duckdb")].status = V1DeploymentStatus(
        observed_generation=1, replicas=1, updated_replicas=1, ready_replicas=0
    )
    rec.add(
        "pods",
        {
            "metadata": {
                "name": "d-1",
                "labels": labels,
                "annotations": {m.POD_ANNOTATION_SET: "p" * 64},
            },
            "status": {
                "phase": "Pending",
                "initContainerStatuses": [
                    {
                        "name": "lb-deps-fetch",
                        "ready": False,
                        "restartCount": 0,
                        "image": "python:3.11-slim",
                        "imageID": "",
                        "state": {"terminated": {"exitCode": 4}},
                    }
                ],
            },
        },
        namespace="ns",
    )
    rec.pod_logs[("ns", "d-1")] = "LB_DEPS_ERROR hash mismatch x.whl expected=a got=b\n"
    # The failure is this attempt's (it appears after the pre-wait cleanup).
    with patch("lakebench.deploy.deps._delete_stale_consumer_pods"):
        with pytest.raises(RuntimeError, match="hash mismatch"):
            wait_consumer_rolled(
                "ns", "lakebench-duckdb", "p" * 64, timeout_seconds=60, what="DuckDB"
            )


def test_an_unreachable_server_is_not_fatal_at_once(recording_k8s):
    """Exit 3 (the server was restarting) is retried by kubelet: the wait
    keeps going instead of failing on the first poll."""
    from lakebench.deploy.deps import wait_consumer_rolled

    rec = recording_k8s
    pod = _consumer_cluster(rec, exit_code=3, log="LB_DEPS_ERROR cannot fetch x after 3 attempts\n")
    rec.add("pods", pod, namespace="ns")
    with (
        patch("lakebench.deploy.deps._delete_stale_consumer_pods"),
        patch("lakebench.k8s.wait.time.sleep"),
        pytest.raises(RuntimeError, match="Timeout"),
    ):
        wait_consumer_rolled("ns", "lakebench-duckdb", "p" * 64, timeout_seconds=1, what="DuckDB")


def test_the_driver_downloads_into_a_bounded_tmp_and_waits_for_its_set():
    cfg = make_config()
    mgr = _manager(cfg)
    spec = mgr._build_manifest(JobType.SILVER_BUILD)["spec"]
    drv = spec["driver"]["template"]["spec"]
    vols = {v["name"]: v for v in drv["volumes"]}
    assert vols["lb-deps-dl"]["emptyDir"]["sizeLimit"] == "5Gi"
    mounts = {v["mountPath"]: v["name"] for v in drv["containers"][0]["volumeMounts"]}
    assert mounts["/tmp"] == "lb-deps-dl"
    (ready,) = [c for c in drv["initContainers"] if c["name"] == "lb-deps-ready"]
    script = ready["command"][-1]
    assert f"{urlsplit(mgr.deps.base_url).hostname}:{m.PORT}/ready" in script
    assert mgr.deps.pinset_sha256 in script
    compile(script, "lb-deps-ready", "exec")  # the init container's Python parses


def test_consumer_init_containers_mount_what_they_read():
    for recipe, template in (
        ("hive-iceberg-spark-thrift", "spark-thrift/sparkapplication.yaml.j2"),
        ("hive-iceberg-spark-duckdb", "duckdb/deployment.yaml.j2"),
    ):
        dep, _ = _render(make_config(recipe=recipe), template)
        spec = dep["spec"]["template"]["spec"]
        (fetch,) = [c for c in spec["initContainers"] if c["name"] == "lb-deps-fetch"]
        mounted = {v["name"]: v["mountPath"] for v in fetch["volumeMounts"]}
        assert mounted["lb-deps-tools"] == m.TOOLS_MOUNT
        assert mounted["lb-deps-manifest"] == m.MANIFEST_MOUNT
        if "duckdb" in recipe:
            main = {v["name"]: v["mountPath"] for v in spec["containers"][0]["volumeMounts"]}
            # Without it the extensions the init container fetched are invisible.
            assert main["duckdb-ext"] == "/tmp/.duckdb/extensions"
            assert main["lb-deps-py"] == "/opt/lb-deps/py"


def test_an_unrelated_connection_error_is_not_blamed_on_the_set():
    log = (
        "INFO Utils: Fetching http://lb-deps.ns.svc.cluster.local:8080/sets/a/jars/x.jar\n"
        "INFO SparkContext: Running Spark\n"
        "java.net.ConnectException: Connection refused\n"
        "\tat org.apache.hadoop.hive.metastore.HiveMetaStoreClient.open\n"
    )
    assert classify_dependency_failure(log) is None
