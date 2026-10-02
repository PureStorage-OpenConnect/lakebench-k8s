"""The run-start checks and the run's dependency provenance (DEP-2, ch01 s2.8).

``deps.runtime.load_handle`` runs against the recording fake with the real
code (marker ``real_deps``); the CLI wiring is pinned by a static test and by
the exit-code scenarios in test_exit_codes.py.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakebench.deps import manifest as m
from lakebench.deps import runtime
from lakebench.deps.request import select_request
from lakebench.exit_codes import PrerequisiteError, SafetyRefusal
from tests.conftest import make_config
from tests.test_deps_manifest import fake_shown

NS = "rd1"
SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"
pytestmark = pytest.mark.real_deps


def _cfg(recipe="hive-iceberg-spark-trino"):
    s3 = {"endpoint": "http://minio:9000", "access_key": "k", "secret_key": "s"}
    return make_config(name=NS, recipe=recipe, platform={"storage": {"s3": s3}})


def _server_pod(sha, uid="uid-srv", ready=True):
    return {
        "metadata": {
            "name": "lb-deps-x",
            "uid": uid,
            "labels": dict(m.SELECTOR_LABELS),
            "annotations": {m.POD_ANNOTATION_REQUEST: sha},
        },
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }


def _deployed(rec, cfg, *, annotate=True, shown=None, uid="uid-srv", pod=True):
    """A namespace as a successful deps step leaves it."""
    request = select_request(cfg)
    shown = shown or fake_shown(request)
    anns = {m.ANNOTATION_DEPS_SET: shown["pinset_sha256"]} if annotate else {}
    rec.for_config(cfg)
    rec.add_namespace(NS, annotations=anns)
    rec.add(
        "configmaps",
        {
            "metadata": {"name": m.MANIFEST_CONFIGMAP},
            "data": {"manifest.json": json.dumps(shown), "server-pod-uid": "uid-srv"},
        },
        namespace=NS,
    )
    rec.add(
        "configmaps",
        {
            "metadata": {"name": m.tools_configmap_name(request.request_sha256)},
            "data": {"request.json": request.canonical_json()},
        },
        namespace=NS,
    )
    if pod:
        rec.add("pods", _server_pod(request.request_sha256, uid=uid), namespace=NS)
    return request, shown


def _k8s():
    from lakebench.k8s.client import K8sClient

    return K8sClient(namespace=NS)


def test_a_verified_set_gives_the_handle(recording_k8s):
    cfg = _cfg()
    request, shown = _deployed(recording_k8s, cfg)
    h = runtime.load_handle(cfg, _k8s())
    assert (h.pinset_sha256, h.request_sha256) == (shown["pinset_sha256"], request.request_sha256)
    assert h.base_url == f"http://lb-deps.{NS}.svc.cluster.local:8080/sets/{h.pinset_sha256}"
    assert not recording_k8s.mutations()  # a check, nothing written


def test_run_refuses_without_server(recording_k8s):
    cfg = _cfg()
    recording_k8s.for_config(cfg)
    recording_k8s.add_namespace(NS)
    with pytest.raises(PrerequisiteError) as e:
        runtime.load_handle(cfg, _k8s())
    assert e.value.path == "run.deps_missing" and "no dependency server" in e.value.what


def test_run_refuses_an_unfinished_deploy(recording_k8s):
    cfg = _cfg()
    _deployed(recording_k8s, cfg, annotate=False)
    with pytest.raises(PrerequisiteError) as e:
        runtime.load_handle(cfg, _k8s())
    assert e.value.path == "run.deps_stale" and "not verified" in e.value.what


def test_run_refuses_changed_request_and_names_the_change(recording_k8s):
    deployed = _cfg()
    _deployed(recording_k8s, deployed)
    now = make_config(
        name=NS,
        images={"spark": "apache/spark:4.1.1-python3"},
        platform={
            "storage": {"s3": {"endpoint": "http://m:9000", "access_key": "k", "secret_key": "s"}}
        },
    )
    with pytest.raises(PrerequisiteError) as e:
        runtime.load_handle(now, _k8s())
    assert e.value.path == "run.deps_stale"
    assert "spark_image" in e.value.why


def test_run_refuses_a_manifest_on_another_pinset(recording_k8s):
    cfg = _cfg()
    request = select_request(cfg)
    shown = fake_shown(request)
    _deployed(recording_k8s, cfg, shown=shown)
    ns = recording_k8s.store[("namespaces", None, NS)]
    ns.metadata.annotations = {m.ANNOTATION_DEPS_SET: "0" * 64}
    with pytest.raises(SafetyRefusal) as e:
        runtime.load_handle(cfg, _k8s())
    assert e.value.path == "run.deps_mismatch"


def test_run_refuses_a_server_without_a_ready_pod(recording_k8s):
    cfg = _cfg()
    _deployed(recording_k8s, cfg, pod=False)
    with pytest.raises(PrerequisiteError) as e:
        runtime.load_handle(cfg, _k8s())
    assert e.value.path == "run.deps_stale" and "no Ready pod" in e.value.what


def test_run_refuses_replaced_server_with_other_set(recording_k8s):
    cfg = _cfg()
    request, shown = _deployed(recording_k8s, cfg, uid="uid-new")
    other = fake_shown(request)
    other["jar_order"] = list(reversed(other["jar_order"]))
    recording_k8s.on_command("kubectl", "exec", stdout=json.dumps(other))
    with pytest.raises(SafetyRefusal) as e:
        runtime.load_handle(cfg, _k8s())
    assert e.value.path == "run.deps_mismatch" and "replaced" in e.value.what


def test_a_replaced_server_on_the_same_set_is_accepted(recording_k8s):
    cfg = _cfg()
    _request, shown = _deployed(recording_k8s, cfg, uid="uid-new")
    recording_k8s.on_command("kubectl", "exec", stdout=json.dumps(shown))
    assert runtime.load_handle(cfg, _k8s()).server_pod_uid == "uid-new"


def test_run_refuses_thrift_on_old_set(recording_k8s):
    cfg = _cfg("hive-iceberg-spark-thrift")
    _deployed(recording_k8s, cfg)
    labels = {
        "app.kubernetes.io/name": "lakebench",
        "app.kubernetes.io/component": "spark-thrift-server",
    }
    recording_k8s.add(
        "deployments",
        {"metadata": {"name": "lakebench-spark-thrift"}, "spec": {"selector": {"matchLabels": labels}, "template": {}}},
        namespace=NS,
    )  # fmt: skip
    recording_k8s.add(
        "pods",
        {"metadata": {"name": "thrift-1", "labels": labels, "annotations": {m.POD_ANNOTATION_SET: "f" * 64}}},
        namespace=NS,
    )  # fmt: skip
    with pytest.raises(SafetyRefusal) as e:
        runtime.load_handle(cfg, _k8s())
    assert e.value.path == "run.deps_mismatch" and "thrift-1" in e.value.why


# --- the run-end check and provenance -----------------------------------------------


def test_pod_check_lists_mismatches_and_records_failures(recording_k8s, monkeypatch):
    """The run-end check reads the query engine pods (the drivers carry the
    run's own handle and cannot differ)."""
    cfg = _cfg("hive-iceberg-spark-duckdb")
    h = m.placeholder_handle(cfg)
    recording_k8s.for_config(cfg)
    recording_k8s.add_namespace(NS)
    labels = {"app.kubernetes.io/component": "duckdb"}
    recording_k8s.add(
        "deployments",
        {"metadata": {"name": "lakebench-duckdb"}, "spec": {"selector": {"matchLabels": labels}, "template": {}}},
        namespace=NS,
    )  # fmt: skip
    for name, pinset in (("duck-ok", h.pinset_sha256), ("duck-old", "e" * 64)):
        recording_k8s.add(
            "pods",
            {"metadata": {"name": name, "labels": labels, "annotations": {m.POD_ANNOTATION_SET: pinset}}},
            namespace=NS,
        )  # fmt: skip
    out = runtime.check_pods(cfg, h, since=None)
    assert out["pods_checked"] == 2
    assert [p["pod"] for p in out["pod_mismatches"]] == ["duck-old"]
    monkeypatch.setattr("time.sleep", lambda s: None)
    recording_k8s.fail("list", "pods", status=500, times=1)
    assert runtime.check_pods(cfg, h, since=None)["pods_checked"] == 2  # a blip is retried
    recording_k8s.fail("list", "pods", status=500)
    broken = runtime.check_pods(cfg, h, since=None)
    assert broken["pods_checked"] is None and broken["pods_check_error"]


def test_an_unchecked_query_engine_fails_the_verdict():
    from lakebench.metrics.verdict import compute_verdict

    metrics = _pipeline_metrics()
    metrics.provenance = {
        "deps": {"pods_checked": None, "pods_check_error": "500", "pod_mismatches": []}
    }
    v = compute_verdict(metrics)
    assert v.gates.get("deps") == "FAIL" and any("not checked" in r for r in v.reasons)


def test_pod_set_mismatch_fails_verdict():
    from lakebench.metrics.verdict import compute_verdict

    metrics = _pipeline_metrics()
    metrics.provenance = {"deps": {"pod_mismatches": [{"pod": "thrift-1", "pinset": "f" * 64}]}}
    v = compute_verdict(metrics)
    assert str(getattr(v.status, "value", v.status)) == "FAILED"
    assert v.gates.get("deps") == "FAIL"
    assert any("pods ran different dependency sets" in r for r in v.reasons)
    metrics.provenance = {"deps": {"pod_mismatches": []}}
    assert compute_verdict(metrics).gates.get("deps") is None


def _pipeline_metrics():
    from lakebench._clock import utc_now
    from lakebench.metrics.collector import PipelineMetrics

    return PipelineMetrics(
        run_id="r", deployment_name="d", start_time=utc_now(), config_snapshot={}, success=True
    )


def test_provenance_block_shape():
    cfg = make_config(workload={"schema": "financial"})
    block = m.provenance_block(m.placeholder_handle(cfg))
    assert set(block) >= {
        "pinset_sha256",
        "request_sha256",
        "repositories",
        "pypi_index",
        "groups",
        "python",
        "overlaps",
        "resolved_at",
        "server_pod",
        "pods_checked",
        "pod_mismatches",
    }
    assert set(block["groups"]) == {"jars", "py-reference"}


# --- every submitting CLI path loads the set first --------------------------------


def test_every_cli_engine_gets_the_set_before_any_submit():
    """Each function in cli/ that builds a Spark job manager loads the
    deployment's set (load_deps_handle) and assigns ``.deps`` to it."""
    problems = []
    for path in sorted((SRC / "cli").glob("*.py")):
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef):
                continue
            calls = [
                c
                for c in ast.walk(fn)
                if isinstance(c, ast.Call)
                and getattr(c.func, "id", getattr(c.func, "attr", "")) == "get_engine"
            ]
            if not calls:
                continue
            src = ast.unparse(fn)
            if "load_deps_handle" not in src and "deps_handle" not in src:
                problems.append(f"{path.name}:{fn.name} builds an engine without the set")
            if ".deps = " not in src:
                problems.append(f"{path.name}:{fn.name} never assigns job_manager.deps")
    assert problems == []


def test_results_are_not_attached_to_a_run_on_another_set(recording_k8s):
    """`benchmark` and `query` refuse to add results measured on a query
    engine that a redeploy moved to another set."""
    cfg = _cfg("hive-iceberg-spark-duckdb")
    recording_k8s.for_config(cfg)
    recording_k8s.add_namespace(NS)
    labels = {"app.kubernetes.io/component": "duckdb"}
    recording_k8s.add(
        "deployments",
        {"metadata": {"name": "lakebench-duckdb"}, "spec": {"selector": {"matchLabels": labels}, "template": {}}},
        namespace=NS,
    )  # fmt: skip
    recording_k8s.add(
        "pods",
        {"metadata": {"name": "duck-1", "labels": labels, "annotations": {m.POD_ANNOTATION_SET: "b" * 64}}},
        namespace=NS,
    )  # fmt: skip
    run = SimpleNamespace(run_id="r1", provenance={"deps": {"pinset_sha256": "a" * 64}})
    assert "duck-1" in runtime.attach_refusal(cfg, run)
    same = SimpleNamespace(run_id="r1", provenance={"deps": {"pinset_sha256": "b" * 64}})
    assert runtime.attach_refusal(cfg, same) is None
    old = SimpleNamespace(run_id="r0", provenance={})
    assert runtime.attach_refusal(cfg, old) is None


def test_the_record_keeps_the_pod_check_when_the_job_manager_is_read_again():
    """The collector re-reads the job manager at run end; a deps block on the
    same set keeps its pod check, and a manager holding the set's handle
    records the set, not "not_recorded"."""
    from types import SimpleNamespace

    from lakebench.metrics import provenance as prov_mod
    from lakebench.metrics.collector import MetricsCollector

    cfg = make_config()
    handle = m.placeholder_handle(cfg)
    assert prov_mod.job_manager_fields(SimpleNamespace(deps=handle))["deps"] == (
        m.provenance_block(handle)
    )
    coll = MetricsCollector()
    run = coll.start_run("20261002-000000-dddddd", "d", {})
    run.provenance["deps"] = {**m.provenance_block(handle), "pods_checked": 2}
    coll.record_job_manager(SimpleNamespace(scripts_provenance=None, deps=handle))
    coll.end_run(success=True)
    assert run.provenance["deps"]["pods_checked"] == 2


@pytest.mark.parametrize("other", ["none", "another-set"])
def test_the_job_manager_never_replaces_the_recorded_block(other):
    """A job manager without the set, or on another set, cannot turn a
    mismatched run into a pass: the block stays, and another set fails the
    verdict."""
    from types import SimpleNamespace

    from lakebench.metrics.collector import MetricsCollector
    from lakebench.metrics.verdict import _deps_pods_reason

    cfg = make_config()
    handle = m.placeholder_handle(cfg)
    coll = MetricsCollector()
    run = coll.start_run("20261002-000000-eeeeee", "d", {})
    block = {
        **m.provenance_block(handle),
        "pods_checked": 1,
        "pod_mismatches": [{"pod": "thrift-0", "pinset": "f" * 64}],
    }
    run.provenance["deps"] = block
    deps = None if other == "none" else m.DepsHandle("e" * 64, "r" * 64, "", "", handle.manifest)
    coll.record_job_manager(SimpleNamespace(scripts_provenance=None, deps=deps))
    coll.end_run(success=True)
    got = run.provenance["deps"]
    assert got["pinset_sha256"] == handle.pinset_sha256
    assert got["pod_mismatches"] and _deps_pods_reason(run)
    if other == "another-set":
        assert got["job_manager_pinset"] == "e" * 64
