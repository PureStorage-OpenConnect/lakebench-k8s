"""Run provenance is complete (EVD-5, DESIGN ch03 section 5).

A pip-installed run names the commit its wheel was built from; the record
says which install it was, re-reads the code at run end, names the config
file, the scripts ConfigMaps, the dependency set (or that none was
recorded), the image digests the pods ran and the scratch the cluster held.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.metrics import provenance as prov_mod
from lakebench.metrics.collector import MetricsCollector

REPO = Path(__file__).resolve().parent.parent
SHA = "0123456789abcdef0123456789abcdef01234567"


def _git_head(path: Path) -> str | None:
    r = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, capture_output=True, text=True, check=False
    )
    return r.stdout.strip() if r.returncode == 0 else None


# --- the wheel names its commit ----------------------------------------------


def test_wheel_names_commit(tmp_path):
    """``python -m build`` (sdist, then the wheel from the unpacked sdist,
    as CI and the release build it): the installed wheel's provenance names
    the commit the tree was at. Without the hook it names none."""
    pytest.importorskip("build")
    pytest.importorskip("hatchling")
    head = _git_head(REPO)
    if head is None or shutil.which("git") is None:
        pytest.skip("not a git checkout")
    out = tmp_path / "dist"
    r = subprocess.run(
        [sys.executable, "-m", "build", "--no-isolation", "--outdir", str(out), str(REPO)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-2000:]
    (wheel,) = out.glob("*.whl")
    site = tmp_path / "site"
    with zipfile.ZipFile(wheel) as z:
        z.extractall(site)
    # -S: no site-packages, so the shared editable install cannot answer.
    # The module is loaded from the installed file alone (the package's
    # dependencies are not installed here).
    probe = (
        "import importlib.util, json, sys; "
        "spec = importlib.util.spec_from_file_location('p', sys.argv[1]); "
        "m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); "
        "print(json.dumps(m.sample()))"
    )
    r = subprocess.run(
        [sys.executable, "-S", "-c", probe, str(site / "lakebench" / "metrics" / "provenance.py")],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
    )
    assert r.returncode == 0, r.stderr
    prov = json.loads(r.stdout)
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", "src/lakebench"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert prov["install"] == "wheel"
    assert prov["git_sha"] == head
    assert prov["git_dirty"] is bool(status)


def _hook_module():
    pytest.importorskip("hatchling")
    spec = importlib.util.spec_from_file_location("lb_hatch_build", REPO / "hatch_build.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _hook(mod, root: Path, target: str):
    return mod.CustomBuildHook(str(root), {}, None, None, str(root / "dist"), target)


def test_hook_outside_a_checkout_writes_unknown(tmp_path):
    mod = _hook_module()
    (tmp_path / "src" / "lakebench").mkdir(parents=True)
    assert mod.build_info_source(str(tmp_path)) is None
    data: dict = {}
    hook = _hook(mod, tmp_path, "wheel")
    hook.initialize("standard", data)
    (src,) = data["force_include"]
    assert data["force_include"][src] == "lakebench/_build_info.py"
    info = prov_mod.read_build_info(Path(src))
    hook.finalize("standard", data, "")
    assert info == {"git_sha": None, "git_dirty": None}


def test_hook_keeps_an_unpacked_sdists_file(tmp_path):
    mod = _hook_module()
    pkg = tmp_path / "src" / "lakebench"
    pkg.mkdir(parents=True)
    (tmp_path / "PKG-INFO").write_text("Metadata-Version: 2.4\n")
    (pkg / "_build_info.py").write_text(mod.render(SHA, False))
    data: dict = {}
    _hook(mod, tmp_path, "wheel").initialize("standard", data)
    assert data["force_include"] == {str(pkg / "_build_info.py"): "lakebench/_build_info.py"}


def test_hook_refuses_a_stale_file_in_a_source_tree(tmp_path):
    mod = _hook_module()
    pkg = tmp_path / "src" / "lakebench"
    pkg.mkdir(parents=True)
    (pkg / "_build_info.py").write_text(mod.render(SHA, False))
    with pytest.raises(RuntimeError, match="generated at build time"):
        _hook(mod, tmp_path, "wheel").initialize("standard", {})


def test_hook_skips_editable_installs(tmp_path):
    mod = _hook_module()
    data: dict = {}
    _hook(mod, tmp_path, "wheel").initialize("editable", data)
    assert data == {}


# --- reading the install -----------------------------------------------------


def test_read_build_info_parses_never_imports(tmp_path):
    f = tmp_path / "_build_info.py"
    assert prov_mod.read_build_info(f) is None
    f.write_text(f"GIT_SHA = {SHA!r}\nGIT_DIRTY = False\nraise SystemExit(3)\n")
    assert prov_mod.read_build_info(f) == {"git_sha": SHA, "git_dirty": False}
    f.write_text("GIT_SHA = 'not-a-sha'\nGIT_DIRTY = 'no'\n")
    assert prov_mod.read_build_info(f) == {"git_sha": None, "git_dirty": None}
    f.write_text("this is not python (")
    assert prov_mod.read_build_info(f) == {"git_sha": None, "git_dirty": None}


def _fake_package(tmp_path: Path, monkeypatch, build_info: str | None) -> Path:
    pkg = tmp_path / "site" / "lakebench"
    (pkg / "metrics").mkdir(parents=True)
    (pkg / "__init__.py").write_text('__version__ = "9.9.9"\n')
    if build_info is not None:
        (pkg / "_build_info.py").write_text(build_info)
    fake = pkg / "metrics" / "provenance.py"
    fake.write_text("")
    monkeypatch.setattr(prov_mod, "__file__", str(fake))
    return pkg


def test_installed_package_reads_its_build_info(tmp_path, monkeypatch):
    pkg = _fake_package(tmp_path, monkeypatch, f"GIT_SHA = {SHA!r}\nGIT_DIRTY = False\n")
    s = prov_mod.sample()
    assert s == {
        "lakebench_version": "9.9.9",
        "git_sha": SHA,
        "git_dirty": False,
        "install": "wheel",
        "tree_sha256": prov_mod.tree_sha256(pkg),
    }
    assert len(s["tree_sha256"]) == 64


def test_tree_hash_sees_an_edit_git_cannot(tmp_path, monkeypatch):
    """An edited file in an installed wheel: the build info still names the
    same commit, the file hash moves, and the run reads as changed."""
    pkg = _fake_package(tmp_path, monkeypatch, f"GIT_SHA = {SHA!r}\nGIT_DIRTY = False\n")
    (pkg / "__pycache__").mkdir()
    (pkg / "__pycache__" / "x.cpython-311.pyc").write_bytes(b"1")
    start = prov_mod.sample()
    (pkg / "__pycache__" / "x.cpython-311.pyc").write_bytes(b"2")  # bytecode: ignored
    assert prov_mod.end_sample(start)["code_changed_during_run"] is False
    (pkg / "metrics" / "collector.py").write_text("# edited\n")
    end = prov_mod.end_sample(start)
    assert end["git_sha"] == start["git_sha"]
    assert end["code_changed_during_run"] is True


def test_failed_git_read_at_end_is_not_a_change_when_files_match():
    start = {
        "lakebench_version": "1.7.0",
        "git_sha": SHA,
        "git_dirty": False,
        "install": "checkout",
        "tree_sha256": "t" * 64,
    }
    assert prov_mod.code_changed(start, {**start, "git_sha": None, "git_dirty": None}) is False
    assert prov_mod.code_changed(start, {**start, "git_sha": "f" * 40}) is True
    assert prov_mod.code_changed(start, {**start, "tree_sha256": "u" * 64}) is True
    assert prov_mod.code_changed(start, {**start, "lakebench_version": "1.7.1"}) is True
    # No file hash on one side: every code field counts, a failed read too.
    no_tree = {**start, "tree_sha256": None}
    assert prov_mod.code_changed(no_tree, {**no_tree, "git_sha": None}) is True


def test_each_run_samples_its_own_start(monkeypatch):
    """run --repeat runs several runs in one process: each start is read
    afresh, not the first run's."""
    calls = iter([{"git_sha": "a" * 40}, {"git_sha": "b" * 40}])
    monkeypatch.setattr(prov_mod, "sample", lambda: dict(next(calls)))
    assert prov_mod.run_provenance()["git_sha"] == "a" * 40
    assert prov_mod.run_provenance()["git_sha"] == "b" * 40


def test_package_without_build_info_is_unknown(tmp_path, monkeypatch):
    _fake_package(tmp_path, monkeypatch, None)
    s = prov_mod.sample()
    assert (s["install"], s["git_sha"], s["git_dirty"]) == ("unknown", None, None)


def test_checkout_install_is_named():
    s = prov_mod.sample()
    if _git_head(Path(prov_mod.__file__).parent) is None:
        pytest.skip("not run from a checkout")
    assert s["install"] == "checkout"


# --- run end -----------------------------------------------------------------


def test_end_sample_detects_change(monkeypatch):
    coll = MetricsCollector()
    supported = {"state": "supported", "basis": "validated", "validation_runs": ["r1"]}
    snapshot = {"experiment_inputs": {"support": dict(supported)}}
    start = {"lakebench_version": "1.7.0", "git_sha": SHA, "git_dirty": False, "install": "wheel"}
    monkeypatch.setattr(prov_mod, "run_provenance", lambda: dict(start))
    monkeypatch.setattr("lakebench.metrics.collector.run_provenance", lambda: dict(start))
    run = coll.start_run("20261001-000000-aaaaaa", "d", snapshot)
    monkeypatch.setattr(prov_mod, "sample", lambda: {**start, "git_sha": "f" * 40})
    coll.end_run(success=True)
    end = run.provenance["end_sample"]
    assert end["git_sha"] == "f" * 40
    assert end["code_changed_during_run"] is True
    support = run.config_snapshot["experiment_inputs"]["support"]
    assert support["state"] == "unverified"
    assert "code changed during the run" in support["basis"]
    assert "validation_runs" not in support


def test_unchanged_code_keeps_support(monkeypatch):
    coll = MetricsCollector()
    supported = {"state": "supported", "basis": "validated"}
    start = {"lakebench_version": "1.7.0", "git_sha": SHA, "git_dirty": False, "install": "wheel"}
    monkeypatch.setattr("lakebench.metrics.collector.run_provenance", lambda: dict(start))
    monkeypatch.setattr(prov_mod, "sample", lambda: dict(start))
    run = coll.start_run(
        "20261001-000000-bbbbbb", "d", {"experiment_inputs": {"support": dict(supported)}}
    )
    coll.end_run(success=True)
    assert run.provenance["end_sample"]["code_changed_during_run"] is False
    assert run.config_snapshot["experiment_inputs"]["support"] == supported


def test_support_state_refuses_supported_when_code_changed():
    from lakebench.config.support import Validation, support_state

    key = ("customer360", "hive-iceberg-spark-trino", "batch")
    record = {key: Validation(*key, tree="t", runs=("r1",))}
    clean = {"git_sha": SHA, "git_dirty": False}
    args = ("customer360", "hive", "iceberg", "spark", "trino", "batch")
    assert support_state(*args, record=record, provenance=clean)["state"] == "supported"
    changed = {**clean, "end_sample": {"git_sha": "f" * 40, "code_changed_during_run": True}}
    out = support_state(*args, record=record, provenance=changed)
    assert out["state"] == "unverified"


# --- config, scripts, deps ---------------------------------------------------


def test_config_sha256_is_the_snapshots_and_path_is_absolute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run = MetricsCollector().start_run(
        "20261001-000000-cccccc", "d", {"config_sha256": "e" * 64}, config_path="cfg.yaml"
    )
    assert run.provenance["config_sha256"] == "e" * 64
    assert run.provenance["config_path"] == str(tmp_path / "cfg.yaml")


def test_deps_absent_not_recorded():
    coll = MetricsCollector()
    run = coll.start_run("20261001-000000-dddddd", "d", {})
    assert run.provenance["deps"] == "not_recorded"
    coll.record_job_manager(SimpleNamespace())  # no scripts_provenance, no deps
    assert run.provenance["deps"] == "not_recorded"
    assert "scripts_sha256" not in run.provenance


def test_deps_set_after_the_scripts_are_read_at_run_end():
    coll = MetricsCollector()
    run = coll.start_run("20261001-000000-dddddf", "d", {})
    mgr = SimpleNamespace(scripts_provenance=None, deps=None)
    coll.record_job_manager(mgr)
    mgr.deps = {"pinset_sha256": "d" * 64}
    coll.end_run(success=True)
    assert run.provenance["deps"] == {"pinset_sha256": "d" * 64}


def test_job_manager_scripts_and_deps_recorded():
    coll = MetricsCollector()
    run = coll.start_run("20261001-000000-eeeeee", "d", {})
    mgr = SimpleNamespace(
        scripts_provenance={
            "scripts_sha256": "a" * 64,
            "scripts_maps": {"common": "b" * 64},
            "files_sha256": {"common.py": "c" * 64},
        },
        deps={"pinset_sha256": "d" * 64},
    )
    coll.record_job_manager(mgr)
    assert run.provenance["scripts_sha256"] == "a" * 64
    assert run.provenance["scripts_maps"] == {"common": "b" * 64}
    assert run.provenance["scripts_files_sha256"] == {"common.py": "c" * 64}
    assert run.provenance["deps"] == {"pinset_sha256": "d" * 64}


def test_a_new_job_manager_has_no_deps_yet():
    from lakebench.spark.job import SparkJobManager

    mgr = SparkJobManager.__new__(SparkJobManager)
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    SparkJobManager.__init__(mgr, MagicMock(get_namespace=lambda: "ns"), k8s)
    assert prov_mod.job_manager_fields(mgr)["deps"] == "not_recorded"


# --- images ------------------------------------------------------------------


def _pod(name, labels, container, image_id, *, phase="Running", deleting=False):
    cs = SimpleNamespace(name=container, image_id=image_id)
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            labels=labels,
            deletion_timestamp="2026-10-01T00:00:00Z" if deleting else None,
        ),
        status=SimpleNamespace(phase=phase, container_statuses=[cs]),
    )


def _pods(*pods):
    return SimpleNamespace(items=list(pods))


APP = "sparkoperator.k8s.io/app-name"
DRIVER = {"spark-role": "driver", APP: "lakebench-silver-build"}
EXECUTOR = {"spark-role": "executor", APP: "lakebench-silver-build"}
TRINO = {"app": "lakebench-trino", "component": "coordinator"}
THRIFT = {"app.kubernetes.io/component": "spark-thrift-server"}
APPS = {"lakebench-silver-build"}


def _observe(prov: dict, pods, at: str = "silver-build", apps=APPS) -> set:
    api = MagicMock()
    api.list_namespaced_pod.return_value = pods
    with patch("kubernetes.client.CoreV1Api", return_value=api):
        seen = prov_mod.observe_images(prov, "ns", at, "2026-10-01T00:00:00+00:00", apps)
    assert api.list_namespaced_pod.call_args.kwargs["_request_timeout"] == (
        prov_mod.POD_LIST_TIMEOUT_S
    )
    return seen


def test_images_observed_per_role_first_seen_wins():
    prov: dict = {}
    seen = _observe(
        prov,
        _pods(
            _pod("d1", DRIVER, "spark-kubernetes-driver", "spark@sha256:1", phase="Succeeded"),
            _pod("e1", EXECUTOR, "spark-kubernetes-executor", "spark@sha256:1"),
            _pod("t1", TRINO, "trino", "trino@sha256:2"),
            _pod("x1", THRIFT, "spark-thrift", "spark@sha256:1"),
            _pod("pg", {"app": "postgres"}, "postgres", "pg@sha256:9"),
            _pod("e0", EXECUTOR, "spark-kubernetes-executor", None),  # not pulled yet
        ),
    )
    assert seen == {"spark_driver", "spark_executor", "trino_coordinator", "thrift"}
    assert prov["images_observed"] == {
        "spark_driver": "spark@sha256:1",
        "spark_executor": "spark@sha256:1",
        "trino_coordinator": "trino@sha256:2",
        "thrift": "spark@sha256:1",
    }
    assert "images_observed_changed" not in prov
    later = {**EXECUTOR, APP: "lakebench-gold-finalize"}
    _observe(
        prov,
        _pods(_pod("e2", later, "spark-kubernetes-executor", "spark@sha256:3")),
        at="gold-finalize",
        apps={"lakebench-gold-finalize"},
    )
    assert prov["images_observed"]["spark_executor"] == "spark@sha256:1"
    (changed,) = prov["images_observed_changed"]
    assert changed["role"] == "spark_executor"
    assert changed["image_id"] == "spark@sha256:3"
    assert changed["at"] == "gold-finalize"


def test_other_runs_pods_are_not_this_runs_images():
    """A namespace keeps an earlier run's finished drivers and may hold
    streams another run left; only this stage's application counts, and
    Trino and Thrift only when running."""
    prov: dict = {}
    old_stream = {"spark-role": "driver", APP: "lakebench-bronze-ingest"}
    seen = _observe(
        prov,
        _pods(
            _pod("lakebench-bronze-ingest-driver", old_stream, "spark-kubernetes-driver", "old@1"),
            _pod("t-old", TRINO, "trino", "trino@old", phase="Succeeded"),
            _pod("d-del", DRIVER, "spark-kubernetes-driver", "spark@del", deleting=True),
            _pod("thrift-exec", {"spark-role": "executor"}, "spark-kubernetes-executor", "t@1"),
        ),
    )
    assert seen == set()
    assert prov["images_observed"] == {}


def test_images_forbidden_reads_not_observed():
    from kubernetes.client.rest import ApiException

    api = MagicMock()
    api.list_namespaced_pod.side_effect = ApiException(status=403, reason="Forbidden")
    prov: dict = {}
    with patch("kubernetes.client.CoreV1Api", return_value=api):
        assert prov_mod.observe_images(prov, "ns", "silver-build", "t", APPS) == set()
    assert prov["images_observed"] == {
        "not_observed": "pod list in ns failed at silver-build: HTTP 403 Forbidden"
    }


def test_a_later_failed_read_keeps_what_was_seen():
    from kubernetes.client.rest import ApiException

    prov: dict = {}
    _observe(prov, _pods(_pod("d1", DRIVER, "spark-kubernetes-driver", "spark@sha256:1")))
    api = MagicMock()
    api.list_namespaced_pod.side_effect = ApiException(status=500, reason="boom")
    with patch("kubernetes.client.CoreV1Api", return_value=api):
        prov_mod.observe_images(prov, "ns", "gold-finalize", "t", APPS)
    assert prov["images_observed"] == {"spark_driver": "spark@sha256:1"}


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_stage_watch_reads_until_driver_and_executor_are_seen():
    """The first RUNNING poll with executors may come before any executor
    has pulled its image; the watch reads again, spaced, at most MAX_READS
    times, and stops once both roles are seen."""
    answers = [{"spark_driver"}, {"spark_driver", "spark_executor"}]
    clock = _Clock()
    watch = prov_mod.StageImageWatch(lambda: answers.pop(0), clock=clock)
    watch.on_status(True, 0)  # no executors yet: no read
    assert watch.reads == 0
    watch.on_status(True, 4)
    assert watch.reads == 1 and not watch.done
    clock.t = 5.0
    watch.on_status(True, 4)  # too soon
    assert watch.reads == 1
    clock.t = 20.0
    watch.on_status(True, 4)
    assert watch.done and watch.reads == 2
    clock.t = 60.0
    watch.on_status(True, 4)
    assert watch.reads == 2
    assert watch.finish() == []


def test_stage_watch_gives_up_and_reports_missing_roles():
    clock = _Clock()
    watch = prov_mod.StageImageWatch(lambda: set(), clock=clock)
    for i in range(10):
        clock.t = i * 100.0
        watch.on_status(True, 4)
    assert watch.reads == prov_mod.StageImageWatch.MAX_READS
    assert watch.finish() == ["spark_driver", "spark_executor"]
    assert watch.reads == prov_mod.StageImageWatch.MAX_READS + 1  # the driver, after the wait


def test_stage_watch_records_missing_roles_in_provenance():
    coll = MetricsCollector()
    run = coll.start_run("20261001-000000-fffff0", "d", {})
    api = MagicMock()
    api.list_namespaced_pod.return_value = _pods(
        _pod("d1", DRIVER, "spark-kubernetes-driver", "spark@sha256:1")
    )
    with patch("kubernetes.client.CoreV1Api", return_value=api):
        watch = coll.stage_image_watch("ns", "lakebench-silver-build", at="silver-build")
        watch.on_status(True, 4)
        assert watch.finish() == ["spark_executor"]
    assert run.provenance["images_observed"] == {"spark_driver": "spark@sha256:1"}
    assert run.provenance["images_observed_missing"] == [
        {"at": "silver-build", "roles": ["spark_executor"]}
    ]


def test_run_end_names_images_never_observed():
    coll = MetricsCollector()
    run = coll.start_run("20261001-000000-ffffff", "d", {})
    coll.end_run(success=True)
    assert "not_observed" in run.provenance["images_observed"]
    assert run.provenance["scratch_as_ran"] == {}
    local = MetricsCollector()
    lrun = local.start_run("20261001-000000-fffff1", "d", {"local": True})
    local.end_run(success=True)
    assert lrun.provenance["images_observed"] == {"not_observed": "local run: no cluster pods"}


# --- scratch as ran ----------------------------------------------------------


def _scratch_config():
    from lakebench.config import LakebenchConfig

    return LakebenchConfig(
        name="t",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                },
                "scratch": {"enabled": True, "storage_class": "px-csi-scratch"},
            }
        },
        architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}},
    )


def test_scratch_as_ran_from_manifest():
    """The SparkApplication the manager builds for silver-build carries its
    profile's 300Gi scratch; the status read returns it, and the record
    holds what the cluster held, not the config's scratch size."""
    from lakebench.modules.pipeline_engines.spark.job import get_job_profile
    from lakebench.spark.job import JobType, SparkJobManager

    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(_scratch_config(), k8s)
    manifest = mgr._build_manifest(JobType.SILVER_BUILD)
    assert get_job_profile("silver-build", "customer360")["scratch_size"] == "300Gi"
    app = {**manifest, "status": {"applicationState": {"state": "COMPLETED"}}}
    api = MagicMock()
    api.get_namespaced_custom_object.return_value = app
    with patch("kubernetes.client.CustomObjectsApi", return_value=api):
        status = mgr.get_job_status("lakebench-silver-build")
    assert status.scratch == {"size_limit": "300Gi", "storage_class": "px-csi-scratch"}
    coll = MetricsCollector()
    run = coll.start_run("20261001-000000-111111", "d", {})
    coll.record_scratch("silver-build", status)
    coll.record_scratch("silver-build", None)  # a later unread status keeps it
    assert run.provenance["scratch_as_ran"]["silver-build"] == {
        "size_limit": "300Gi",
        "storage_class": "px-csi-scratch",
    }


def test_scratch_unread_status_is_not_recorded():
    prov: dict = {}
    prov_mod.record_scratch(prov, "gold-finalize", None)
    assert "not_recorded" in prov["scratch_as_ran"]["gold-finalize"]
    prov_mod.record_scratch(
        prov, "gold-finalize", SimpleNamespace(scratch={"size_limit": None, "storage_class": None})
    )
    assert prov["scratch_as_ran"]["gold-finalize"] == {"size_limit": None, "storage_class": None}


def test_wait_until_running_returns_its_status():
    from lakebench.spark.job import JobState, JobStatus
    from lakebench.spark.monitor import SparkJobMonitor

    m = SparkJobMonitor.__new__(SparkJobMonitor)
    m.namespace = "ns"
    st = JobStatus(name="j", state=JobState.RUNNING, message="", scratch={"size_limit": "1Gi"})
    m.job_manager = MagicMock()
    m.job_manager.get_job_status.return_value = st
    assert m.wait_until_running("j", poll_interval=0).final_status is st


# --- the experiment block: what the identity reads -------------------------


def test_experiment_lakebench_keeps_what_the_identity_reads():
    prov = {
        "lakebench_version": "1.7.0",
        "git_sha": SHA,
        "git_dirty": False,
        "install": "wheel",
        "tree_sha256": "t" * 64,
        "deps": {"pinset_sha256": "d" * 64},
        "images_observed": {"spark_driver": "x@sha256:1"},
        "scripts_files_sha256": {"common.py": "c" * 64},
        "config_path": "/home/someone/cfg.yaml",
        "end_sample": {"code_changed_during_run": False},
    }
    assert prov_mod.experiment_lakebench(prov) == {
        "lakebench_version": "1.7.0",
        "git_sha": SHA,
        "git_dirty": False,
        "install": "wheel",
        "tree_sha256": "t" * 64,
        "deps": {"pinset_sha256": "d" * 64},
        "images_observed": {"spark_driver": "x@sha256:1"},
    }
    # The reason text names the namespace; two unobserved runs read alike.
    a = prov_mod.experiment_lakebench({"images_observed": {"not_observed": "pod list in ns-a"}})
    b = prov_mod.experiment_lakebench({"images_observed": {"not_observed": "pod list in ns-b"}})
    assert a == b == {"images_observed": "not_observed"}
    v16 = {"lakebench_version": "1.6.0", "git_sha": SHA, "git_dirty": False}
    assert prov_mod.experiment_lakebench(v16) == v16


def _v17_record(monkeypatch, deps):
    from lakebench.metrics import build_config_snapshot

    start = {"lakebench_version": "1.7.0", "git_sha": SHA, "git_dirty": False, "install": "wheel"}
    monkeypatch.setattr("lakebench.metrics.collector.run_provenance", lambda: dict(start))
    monkeypatch.setattr(prov_mod, "sample", lambda: dict(start))
    coll = MetricsCollector()
    coll.start_run("20261001-000000-222222", "d", build_config_snapshot(_scratch_config()))
    coll.record_job_manager(SimpleNamespace(scripts_provenance=None, deps=deps))
    run = coll.end_run(success=True)
    return run.to_dict()


def test_saved_record_identity_sees_the_dependency_pinset(monkeypatch):
    """The identity reads the pinset from the experiment block's lakebench
    copy (comparability.dependency_pinset); a saved record must carry it."""
    from lakebench.metrics import comparability as cmp

    d = _v17_record(monkeypatch, {"pinset_sha256": "d" * 64})
    present, value, _ = cmp.dependency_pinset(d["experiment"])
    assert (present, value) == (True, "d" * 64)
    d = _v17_record(monkeypatch, None)
    assert cmp.dependency_pinset(d["experiment"])[1] == cmp.PINSET_NOT_RECORDED


def test_identity_reads_observed_image_digests():
    from lakebench.metrics import comparability as cmp

    exp = {
        "schema": "exp2",
        "identity_version": 2,
        "lakebench": prov_mod.experiment_lakebench(
            {"lakebench_version": "1.7.0", "images_observed": {"spark_driver": "x@sha256:1"}}
        ),
    }
    got = cmp.classify(exp).keys(cmp.ARCHITECTURE)["observed image digests"]
    assert got == {"spark_driver": "x@sha256:1"}


def test_withdrawn_support_reaches_the_saved_block(monkeypatch):
    from lakebench.metrics import build_config_snapshot

    start = {"lakebench_version": "1.7.0", "git_sha": SHA, "git_dirty": False, "install": "wheel"}
    monkeypatch.setattr("lakebench.metrics.collector.run_provenance", lambda: dict(start))
    snapshot = build_config_snapshot(_scratch_config())
    snapshot["experiment_inputs"]["support"] = {"state": "supported", "basis": "validated"}
    coll = MetricsCollector()
    coll.start_run("20261001-000000-333333", "d", snapshot)
    monkeypatch.setattr(prov_mod, "sample", lambda: {**start, "git_sha": "f" * 40})
    d = coll.end_run(success=True).to_dict()
    assert d["experiment"]["support"]["state"] == "unverified"


def _exp2(images):
    return {
        "schema": "exp2",
        "identity_version": 2,
        "lakebench": prov_mod.experiment_lakebench(
            {"lakebench_version": "1.7.0", "images_observed": images}
        ),
    }


def _arch_diff(a, b):
    from lakebench.metrics import comparability as cmp

    diffs = cmp.diff_group(cmp.classify(_exp2(a)), cmp.classify(_exp2(b)), cmp.ARCHITECTURE)
    return [d for d in diffs if d.key == cmp.OBSERVED_IMAGES_KEY]


def test_observed_digests_compare_over_roles_both_sides_saw():
    """Which roles were seen depends on timing; a repeat that missed its
    executors, or could not list pods, is not a different architecture."""
    full = {
        "spark_driver": "r/spark@sha256:1",
        "spark_executor": "r/spark@sha256:1",
        "trino_coordinator": "r/trino@sha256:2",
    }
    assert _arch_diff(full, {"spark_driver": "r/spark@sha256:1"}) == []
    assert _arch_diff(full, {"not_observed": "pod list in ns-b failed"}) == []
    assert _arch_diff({"not_observed": "a"}, {"not_observed": "b"}) == []
    # A registry mirror reports the same digest under another name.
    assert _arch_diff(full, {**full, "spark_driver": "mirror/spark@sha256:1"}) == []
    (d,) = _arch_diff(full, {**full, "spark_executor": "r/spark@sha256:9"})
    assert d.key == "observed image digests"


def test_partial_digest_compare_is_noted():
    from lakebench.metrics import comparability as cmp

    full = {"spark_driver": "r@sha256:1", "spark_executor": "r@sha256:1"}
    assert cmp.observed_images_note(None, None) is None
    assert cmp.observed_images_note(full, dict(full)) is None
    assert cmp.observed_images_note(full, {"spark_driver": "r@sha256:1"}) == (
        "observed image digests compared on spark_driver; not observed on both sides: "
        "spark_executor"
    )
    assert "a side observed none" in cmp.observed_images_note(full, "not_observed")
    assert "no role in common" in cmp.observed_images_note(full, {"thrift": "t@sha256:2"})
