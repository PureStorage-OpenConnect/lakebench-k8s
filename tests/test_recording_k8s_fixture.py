"""Self-tests for the recording K8s fixture (SD-9, G-3).

The fixture is the oracle for SAF-4 ("fails on any unleased shared mutation
or any delete outside the config's namespace") and DEP-3 ("sees zero shared
mutations from deploy"). A fixture that records nothing, or classifies
everything as own, would pass every consumer test and void both acceptances.
These tests prove it records, that it fails on both violation classes through
the code paths Lakebench really uses (``pinned_helm``, ``pinned_kubectl``,
``cluster_lock``, ``S3Client``, ``kubernetes.client`` classes), and that no
call escapes to a real process, API server or object store.
"""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

import pytest

from tests.fixtures.recording_k8s import (
    LEASE,
    OWN,
    READ,
    REASON_CHILD,
    REASON_FOREIGN_BUCKET,
    REASON_FOREIGN_DELETE,
    REASON_LEASE_CLI,
    REASON_LEASE_TAKEN,
    REASON_NOT_CREATED,
    REASON_PATCHED_OVER,
    REASON_UNKNOWN_TOOL,
    REASON_UNLEASED,
    REASON_UNSCRIPTED,
    SHARED,
    K8sRecorder,
    _fixture_body,
    own_secretclass_names,
    recording,
)

NS = "u01"
SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"


def _reasons(rec: K8sRecorder) -> list[str]:
    return [v.reason for v in rec.violations()]


def _core():
    from kubernetes import client

    return client.CoreV1Api()


def _cm(name: str):
    from kubernetes.client.models import V1ConfigMap, V1ObjectMeta

    return V1ConfigMap(metadata=V1ObjectMeta(name=name), data={"k": "v"})


# ---------------------------------------------------------------------------
# The two named cases (DESIGN ch01 3.1)
# ---------------------------------------------------------------------------


class TestNamedCases:
    def test_fixture_flags_unleased_helm_upgrade(self):
        """An operator upgrade through the real pinned helper, outside the lease."""
        from lakebench.k8s._pinned import pinned_helm

        with recording(NS) as rec:
            pinned_helm(
                "ctx-a",
                [
                    "upgrade",
                    "spark-operator",
                    "spark-operator/spark-operator",
                    "-n",
                    "spark-operator",
                ],
                capture_output=True,
                text=True,
            )
            [call] = rec.calls
            assert (call.api, call.verb, call.scope, call.lease_held) == (
                "helm",
                "upgrade",
                SHARED,
                False,
            )
            assert _reasons(rec) == [REASON_UNLEASED]
            with pytest.raises(AssertionError, match="shared mutation without the cluster lease"):
                rec.assert_clean()

    def test_leased_helm_upgrade_is_clean(self):
        """The same upgrade inside the real ``cluster_lock`` passes."""
        from lakebench.deploy.cluster_lock import cluster_lock
        from lakebench.k8s._pinned import pinned_helm

        with recording(NS) as rec:
            with cluster_lock(_core(), timeout=0):
                assert rec.lease_held
                pinned_helm("ctx-a", ["upgrade", "spark-operator", "chart", "-n", "spark-operator"])
            assert not rec.lease_held
            helm = [c for c in rec.calls if c.api == "helm"]
            assert len(helm) == 1 and helm[0].lease_held and helm[0].scope == SHARED
            rec.assert_clean()

    def test_fixture_flags_foreign_delete(self):
        """Deleting B's objects fails, through every channel, lease or not."""
        from lakebench.deploy.cluster_lock import cluster_lock
        from lakebench.k8s._pinned import pinned_kubectl

        with recording(NS) as rec:
            rec.add_namespace(NS)
            rec.add_namespace("u02")
            rec.add("configmaps", _cm("state"), namespace="u02")
            core = _core()
            with cluster_lock(core, timeout=0):
                core.delete_namespaced_config_map("state", "u02")
                core.delete_namespace("u02")
                pinned_kubectl("ctx-a", ["delete", "pvc", "--all", "-n", "u02"])
            deletes = [v for v in rec.violations() if v.reason == REASON_FOREIGN_DELETE]
            assert [v.call.target for v in deletes] == [
                "configmaps/u02/state",
                "namespaces/u02",
                "persistentvolumeclaims/u02/*",
            ]
            # Held under the lease, so these are only foreign-delete violations.
            assert set(_reasons(rec)) == {REASON_FOREIGN_DELETE}
            with pytest.raises(AssertionError, match="delete outside the config's namespace"):
                rec.assert_clean()

    def test_attempted_foreign_delete_counts_even_when_absent(self):
        """A 404 does not excuse the attempt: it is recorded before it runs."""
        from kubernetes.client.exceptions import ApiException

        with recording(NS) as rec:
            with pytest.raises(ApiException) as exc:
                _core().delete_namespace("u02")
            assert exc.value.status == 404
            assert REASON_FOREIGN_DELETE in _reasons(rec)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


class TestClassification:
    def test_own_namespace_work_is_clean(self):
        from kubernetes import client

        with recording(NS) as rec:
            rec.add_stackable()
            core = _core()
            core.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=NS)))
            core.create_namespaced_config_map(NS, _cm("scripts"))
            core.patch_namespace(NS, {"metadata": {"annotations": {"a": "b"}}})
            for sc in sorted(own_secretclass_names(NS)):
                client.CustomObjectsApi().create_cluster_custom_object(
                    "secrets.stackable.tech",
                    "v1alpha1",
                    "secretclasses",
                    {"metadata": {"name": sc}},
                )
                client.CustomObjectsApi().delete_cluster_custom_object(
                    group="secrets.stackable.tech",
                    version="v1alpha1",
                    plural="secretclasses",
                    name=sc,
                )
            core.delete_namespaced_config_map("scripts", NS)
            core.delete_namespace(NS)
            assert {c.scope for c in rec.mutations()} == {OWN}
            rec.assert_clean()
            assert not [k for k in rec.store if k[1] == NS or k[2] == NS]

    def test_secretclass_lookalikes_are_not_own(self):
        """Exact names: B's SecretClass and the legacy fixed names are foreign."""
        from kubernetes import client

        with recording(NS) as rec:
            api = client.CustomObjectsApi()
            for name in (
                "lakebench-s3-credentials-x-u01",  # deployment x-u01 ends in -u01
                "lakebench-s3-credentials-class",  # legacy, shared by every deployment
                "lakebench-s3-ca-cert-class",
            ):
                with pytest.raises(Exception):  # noqa: B017 -- absent: 404
                    api.delete_cluster_custom_object(
                        "secrets.stackable.tech", "v1alpha1", "secretclasses", name
                    )
            assert [v.reason for v in rec.violations()] == [
                REASON_FOREIGN_DELETE,
                REASON_UNLEASED,
            ] * 3

    def test_allow_delete_names_a_target_but_never_waives_the_lease(self):
        from kubernetes import client

        from lakebench.deploy.cluster_lock import cluster_lock

        legacy = "lakebench-s3-credentials-class"
        with recording(NS, allow_delete=[f"secretclasses/{legacy}"]) as rec:
            rec.add_stackable()
            rec.add("secretclasses", {"metadata": {"name": legacy}})
            api = client.CustomObjectsApi()
            with cluster_lock(_core(), timeout=0):
                api.delete_cluster_custom_object(
                    "secrets.stackable.tech", "v1alpha1", "secretclasses", legacy
                )
            rec.assert_clean()
            rec.add("secretclasses", {"metadata": {"name": legacy}})
            api.delete_cluster_custom_object(
                "secrets.stackable.tech", "v1alpha1", "secretclasses", legacy
            )
            assert _reasons(rec) == [REASON_UNLEASED]

    def test_cluster_scoped_kinds_are_shared_whatever_n_says(self):
        """A StorageClass applied with ``-n <own>`` is still cluster state."""
        from kubernetes import client

        from lakebench.k8s._pinned import pinned_kubectl

        manifest = (
            "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata:\n  name: px-scratch\n"
        )
        with recording(NS) as rec:
            pinned_kubectl("ctx-a", ["apply", "-n", NS, "-f", "-"], input=manifest, text=True)
            client.StorageV1Api().create_storage_class({"metadata": {"name": "px-scratch-2"}})
            client.RbacAuthorizationV1Api().create_cluster_role({"metadata": {"name": "cr"}})
            shared = rec.shared_mutations()
            assert [c.target for c in shared] == [
                "storageclasses/px-scratch",
                "storageclasses/px-scratch-2",
                "clusterroles/cr",
            ]
            assert _reasons(rec) == [REASON_UNLEASED] * 3

    def test_operator_namespace_mutations_are_shared(self):
        from lakebench.k8s._pinned import pinned_kubectl, pinned_oc

        with recording(NS) as rec:
            pinned_kubectl(
                "c",
                [
                    "rollout",
                    "restart",
                    "deployment/spark-operator-controller",
                    "-n",
                    "spark-operator",
                ],
            )
            pinned_kubectl(
                "c",
                [
                    "patch",
                    "deployment",
                    "spark-operator-webhook",
                    "-n",
                    "spark-operator",
                    "--type",
                    "json",
                    "-p",
                    "[]",
                ],
            )
            pinned_oc("c", ["adm", "policy", "add-scc-to-user", "anyuid", "-z", "spark", "-n", NS])
            assert [(c.verb, c.scope) for c in rec.calls] == [
                ("rollout restart", SHARED),
                ("patch", SHARED),
                ("adm policy add-scc-to-user", SHARED),
            ]
            assert _reasons(rec) == [REASON_UNLEASED] * 3

    def test_reads_and_dry_runs_never_violate(self):
        from kubernetes import client

        from lakebench.k8s._pinned import pinned_helm, pinned_kubectl

        with recording(NS) as rec:
            rec.add_spark_operator(watched=[NS])
            # A dry run's rendered output matters to its caller: it is scripted.
            rec.on_command("helm", "upgrade", stdout="kind: Deployment\n")
            pinned_helm(
                "c", ["get", "values", "spark-operator", "-n", "spark-operator", "-o", "json"]
            )
            pinned_helm("c", ["status", "spark-operator", "-n", "spark-operator"])
            pinned_helm(
                "c", ["upgrade", "spark-operator", "chart", "--dry-run", "-n", "spark-operator"]
            )
            pinned_kubectl("c", ["get", "pods", "-A"])
            pinned_kubectl("c", ["rollout", "status", "deployment/x", "-n", "spark-operator"])
            rec.on_command("kubectl", "delete", stdout="namespace/u02 deleted (dry run)\n")
            pinned_kubectl("c", ["delete", "ns", "u02", "--dry-run=client"])
            client.CoreV1Api().list_namespace()
            client.AuthorizationV1Api().create_self_subject_access_review({"spec": {}})
            assert rec.calls and all(c.scope == READ for c in rec.calls)
            assert rec.mutations() == []
            rec.assert_clean()

    def test_unknown_kind_and_missing_namespace_fail_closed(self):
        from lakebench.k8s._pinned import pinned_kubectl

        with recording(NS) as rec:
            pinned_kubectl("c", ["delete", "widgets", "w1", "-n", NS])  # unknown kind
            pinned_kubectl("c", ["delete", "configmap", "x"])  # no -n: context default
            pinned_kubectl("c", ["apply", "-f", "/nonexistent/manifest.yaml"])  # unreadable
            assert [c.scope for c in rec.calls] == [SHARED, SHARED, SHARED]
            assert REASON_FOREIGN_DELETE in _reasons(rec)

    def test_unconfigured_recorder_owns_nothing(self):
        with recording() as rec:
            rec.add_namespace("anything")
            _core().create_namespaced_config_map("anything", _cm("x"))
            assert rec.calls[0].scope == SHARED
            assert _reasons(rec) == [REASON_UNLEASED]

    def test_for_config_takes_namespace_and_buckets(self):
        from tests.conftest import make_config

        cfg = make_config(name="dep-a")
        rec = K8sRecorder().for_config(cfg)
        assert rec.namespace == cfg.get_namespace()
        b = cfg.platform.storage.s3.buckets
        assert rec.buckets == {b.bronze, b.silver, b.gold}


# ---------------------------------------------------------------------------
# The lease
# ---------------------------------------------------------------------------


class TestLease:
    def test_real_cluster_lock_drives_lease_held(self):
        from lakebench.deploy.cluster_lock import LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE, cluster_lock

        with recording(NS) as rec:
            assert not rec.lease_held
            with cluster_lock(_core(), timeout=0):
                assert rec.lease_held
                with pytest.raises(Exception):  # noqa: B017 -- namespace absent: 404
                    _core().create_namespaced_config_map("other", _cm("x"))
            assert not rec.lease_held
            lease_calls = [c for c in rec.calls if c.scope == LEASE]
            assert [(c.verb, c.target) for c in lease_calls] == [
                ("create", f"namespaces/{LOCK_NAMESPACE}"),
                ("create", f"configmaps/{LOCK_NAMESPACE}/{LOCK_CONFIGMAP_NAME}"),
                ("delete", f"configmaps/{LOCK_NAMESPACE}/{LOCK_CONFIGMAP_NAME}"),
            ]
            # A shared, non-delete mutation made under the lease is clean.
            assert [c.lease_held for c in rec.shared_mutations()] == [True]
            assert rec.violations() == []

    def test_foreign_lease_is_not_ours(self):
        from lakebench.deploy.cluster_lock import ClusterLockHeld, acquire_cluster_lock

        with recording(NS) as rec:
            rec.seed_lease()
            with pytest.raises(ClusterLockHeld):
                acquire_cluster_lock(_core(), timeout=0)
            assert not rec.lease_held
            rec.add_namespace("spark-operator")
            _core().create_namespaced_config_map("spark-operator", _cm("x"))
            assert _reasons(rec) == [REASON_UNLEASED]

    def test_expired_lease_steal_counts_as_held(self):
        from lakebench.deploy.cluster_lock import cluster_lock

        with recording(NS) as rec:
            rec.seed_lease(acquired_at="2020-01-01T00:00:00+00:00", ttl_seconds=60)
            with cluster_lock(_core(), timeout=0):
                assert rec.lease_held
                assert [c.verb for c in rec.calls if c.scope == LEASE][-1] == "replace"
            assert not rec.lease_held

    def test_steal_mid_hold_drops_lease_held(self):
        """Another process stealing the lease means we no longer hold it."""
        from lakebench.deploy.cluster_lock import cluster_lock
        from lakebench.k8s._pinned import pinned_helm

        with recording(NS) as rec:
            with cluster_lock(_core(), timeout=0):
                rec.seed_lease()
                pinned_helm("c", ["upgrade", "spark-operator", "chart", "-n", "spark-operator"])
            assert _reasons(rec) == [REASON_UNLEASED]


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


class TestS3:
    def _client(self):
        from lakebench.s3.client import S3Client

        return S3Client(endpoint="http://10.0.1.50:80", access_key="k", secret_key="s")

    def test_own_bucket_lifecycle_is_clean(self):
        with recording(NS, buckets=["u01-bronze"]) as rec:
            s3 = self._client()
            assert s3.create_bucket("u01-bronze") is True
            s3.raw_client.put_object(Bucket="u01-bronze", Key="a/b", Body=b"x")
            assert s3.empty_bucket("u01-bronze") >= 0
            assert s3.delete_bucket("u01-bronze") is True
            assert "u01-bronze" not in rec.buckets_store
            rec.assert_clean()

    def test_foreign_bucket_delete_and_write_flagged(self):
        with recording(NS, buckets=["u01-bronze"]) as rec:
            rec.add_bucket("u02-bronze", ["k1"])
            s3 = self._client()
            s3.raw_client.put_object(Bucket="u02-bronze", Key="k2", Body=b"x")
            s3.raw_client.delete_object(Bucket="u02-bronze", Key="k1")
            assert _reasons(rec) == [REASON_FOREIGN_BUCKET, REASON_FOREIGN_DELETE]

    def test_conditional_put_honoured(self):
        from botocore.exceptions import ClientError

        with recording(NS, buckets=["b"]) as rec:
            rec.add_bucket("b")
            raw = self._client().raw_client
            raw.put_object(Bucket="b", Key=".lakebench/owner", Body=b"1", IfNoneMatch="*")
            with pytest.raises(ClientError, match="PreconditionFailed"):
                raw.put_object(Bucket="b", Key=".lakebench/owner", Body=b"2", IfNoneMatch="*")


# ---------------------------------------------------------------------------
# Interception: nothing escapes
# ---------------------------------------------------------------------------


class TestInterception:
    def test_no_real_process_starts(self):
        """conftest's PATH guard exits 97; the fake must answer first."""
        with recording(NS) as rec:
            r = subprocess.run(["kubectl", "delete", "ns", "u02"], capture_output=True, text=True)
            p = subprocess.Popen(["kubectl", "logs", "-f", "x", "-n", NS], stdout=subprocess.PIPE)
            assert r.returncode == 0 and p.wait() == 0
            assert [c.verb for c in rec.calls] == ["delete", "logs"]

    def test_every_api_class_is_replaced(self):
        import kubernetes.client as kc

        with recording(NS) as rec:
            names = [a for a in dir(kc) if a.endswith("Api") and isinstance(getattr(kc, a), type)]
            assert {"CoreV1Api", "AppsV1Api", "CustomObjectsApi", "StorageV1Api"} <= set(names)
            for a in names:
                assert getattr(kc, a).__qualname__ == f"recording_k8s.{a}", a
            kc.AppsV1Api().list_namespaced_deployment("spark-operator")
            assert rec.calls[0].api == "AppsV1Api"

    def test_stream_exec_recorded_with_command(self):
        from kubernetes import client

        with recording(NS) as rec:
            # Imported at call time, as src/ does: an import bound before the
            # patch would reach the real websocket client.
            from kubernetes.stream import stream

            core = client.CoreV1Api()
            stream(core.connect_get_namespaced_pod_exec, "pg-0", "u02", command=["psql", "-c", "x"])
            [call] = rec.calls
            assert call.argv == ("psql", "-c", "x") and call.scope == SHARED
            assert _reasons(rec) == [REASON_UNLEASED]

    def test_assert_no_calls(self):
        with recording(NS) as rec:
            subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True)  # local tool
            rec.assert_no_calls()
            _core().list_namespace()
            with pytest.raises(AssertionError, match="expected no cluster calls"):
                rec.assert_no_calls()


def test_pytest_fixture_asserts_at_teardown():
    from kubernetes.client.exceptions import ApiException

    with pytest.MonkeyPatch.context() as mp:
        gen = _fixture_body(mp)
        rec = next(gen)
        rec.configure(namespace=NS)
        with pytest.raises(ApiException):
            _core().delete_namespace("u02")
        with pytest.raises(AssertionError, match="delete outside the config's namespace"):
            next(gen)


def test_pytest_fixture_clean_teardown(recording_k8s):
    recording_k8s.configure(namespace=NS)
    recording_k8s.add_namespace(NS)
    _core().create_namespaced_config_map(NS, _cm("x"))


# ---------------------------------------------------------------------------
# Store: real Lakebench reads see the seeded cluster
# ---------------------------------------------------------------------------


class TestStore:
    def test_operator_seed_answers_the_real_watch_list_read(self):
        from lakebench.spark import SparkOperatorManager

        with recording(NS) as rec:
            rec.add_spark_operator(watched=["u01", "u02"])
            mgr = SparkOperatorManager(namespace="spark-operator", kube_context="ctx-a")
            assert mgr._get_active_namespaces() == ["u01", "u02"]
            pods = _core().list_namespaced_pod(
                "spark-operator", label_selector="app.kubernetes.io/name=spark-operator"
            )
            assert sorted(p.metadata.name for p in pods.items) == [
                "spark-operator-controller-0",
                "spark-operator-webhook-0",
            ]
            assert all(c.scope == READ for c in rec.calls)

    def test_create_conflict_and_preconditions(self):
        from kubernetes.client.exceptions import ApiException
        from kubernetes.client.models import V1DeleteOptions, V1Preconditions

        with recording(NS) as rec:
            rec.add_namespace(NS)
            core = _core()
            created = core.create_namespaced_config_map(NS, _cm("x"))
            with pytest.raises(ApiException) as e:
                core.create_namespaced_config_map(NS, _cm("x"))
            assert e.value.status == 409
            stale = V1DeleteOptions(preconditions=V1Preconditions(resource_version="0"))
            with pytest.raises(ApiException) as e:
                core.delete_namespaced_config_map("x", NS, body=stale)
            assert e.value.status == 409
            ok = V1DeleteOptions(
                preconditions=V1Preconditions(resource_version=created.metadata.resource_version)
            )
            core.delete_namespaced_config_map("x", NS, body=ok)
            with pytest.raises(ApiException) as e:
                core.create_namespaced_config_map("missing-ns", _cm("y"))
            assert e.value.status == 404

    def test_scripted_command_wins_over_store(self):
        with recording(NS) as rec:
            rec.on_command("helm", "get", "values", stdout='{"a": 1}')
            r = subprocess.run(
                ["helm", "--kube-context", "c", "get", "values", "spark-operator"],
                capture_output=True,
                text=True,
            )
            assert r.stdout == '{"a": 1}'

    def test_injected_api_error(self):
        from kubernetes.client.exceptions import ApiException

        with recording(NS) as rec:
            rec.fail("list", "namespaces", status=503, times=1)
            with pytest.raises(ApiException) as e:
                _core().list_namespace()
            assert e.value.status == 503
            assert _core().list_namespace().items == []


# ---------------------------------------------------------------------------
# Static guards: nothing in src/ can bypass the patches
# ---------------------------------------------------------------------------


_SUBPROCESS_ENTRY = frozenset(
    {"run", "Popen", "call", "check_call", "check_output", "getoutput", "getstatusoutput"}
)
_OS_SPAWN = ("system", "popen", "spawn", "exec", "posix_spawn", "fork")


def _import_time_nodes(tree: ast.Module):
    """Nodes evaluated when the module is imported, before any patch.

    That is the module and class bodies, decorators and default argument
    values; function bodies run at call time and are skipped.
    """
    stack: list[ast.AST] = list(tree.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            if not isinstance(node, ast.Lambda):
                stack.extend(node.decorator_list)
            stack.extend(node.args.defaults)
            stack.extend(d for d in node.args.kw_defaults if d is not None)
            continue
        yield node
        stack.extend(ast.iter_child_nodes(node))


def _aliases(tree: ast.Module) -> dict[str, str]:
    """Local name -> module, for every ``import x`` / ``import x as y``."""
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                out[a.asname or a.name.split(".")[0]] = a.name if a.asname else a.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for a in node.names:
                if node.module == "kubernetes" and a.name in ("client", "stream", "config"):
                    out[a.asname or a.name] = f"kubernetes.{a.name}"
    return out


def _dotted(node: ast.AST, aliases: dict[str, str]) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return ""
    parts.append(aliases.get(node.id, node.id))
    return ".".join(reversed(parts))


def _bypasses(tree: ast.Module) -> list[tuple[int, str]]:
    aliases = _aliases(tree)
    import_time = set(map(id, _import_time_nodes(tree)))
    bad: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith(
                    ("kubernetes.client.api", "kubernetes.dynamic", "kubernetes.watch")
                ) or a.name.startswith(("boto3.", "botocore.session")):
                    bad.append((line, f"import {a.name}"))
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = {a.name for a in node.names}
            mod = node.module
            if mod == "subprocess":
                bad.append((line, f"from subprocess import {sorted(names)}"))
            if mod == "os" and any(n.startswith(_OS_SPAWN) for n in names):
                bad.append((line, f"from os import {sorted(names)}"))
            if mod.startswith(("kubernetes.client.api", "kubernetes.dynamic", "kubernetes.watch")):
                bad.append((line, f"from {mod} import ..."))
            if mod == "kubernetes" and names & {"dynamic", "watch"}:
                bad.append((line, f"from kubernetes import {sorted(names)}"))
            if mod == "kubernetes.client" and any(n.endswith("Api") for n in names):
                bad.append((line, f"from kubernetes.client import {sorted(names)}"))
            if mod == "kubernetes.stream" and id(node) in import_time:
                bad.append((line, "module-level from kubernetes.stream import"))
            if mod.startswith(("boto3", "botocore.session")):
                bad.append((line, f"from {mod} import {sorted(names)}"))
        if isinstance(node, ast.Attribute):
            name = _dotted(node, aliases)
            head, _, attr = name.rpartition(".")
            if head == "os" and attr.startswith(_OS_SPAWN):
                bad.append((line, name))
            if head == "asyncio" and attr.startswith("create_subprocess"):
                bad.append((line, name))
            if name in ("pty.spawn", "boto3.resource", "boto3.Session", "boto3.session.Session"):
                bad.append((line, name))
            if attr in ("call_api", "create_client"):
                bad.append((line, f".{attr}"))
            if "kubernetes.client.api." in name + ".":
                bad.append((line, name))
            if id(node) in import_time and (
                (head == "subprocess" and attr in _SUBPROCESS_ENTRY)
                or name == "boto3.client"
                or (head == "kubernetes.client" and attr.endswith("Api"))
            ):
                bad.append((line, f"import-time reference {name}"))
    return bad


def test_no_source_binds_a_cluster_entry_point_the_fixture_cannot_patch():
    """The fixture patches module attributes, so early bindings would escape it.

    Fails on ``from subprocess import``; ``os.system``/``popen``/``spawn*``/
    ``exec*``/``posix_spawn*`` under any alias; ``asyncio.create_subprocess_*``;
    ``pty.spawn``; ``kubernetes.client.api``, ``kubernetes.dynamic`` and
    ``kubernetes.watch``; ``from kubernetes.client import XApi``; an import-time
    ``from kubernetes.stream import stream``; ``boto3`` other than
    ``boto3.client`` at call time, and ``botocore`` sessions; ``call_api`` and
    ``create_client``; and any import-time reference (an alias such as
    ``RUN = subprocess.run``, a default argument, a module-level ``XApi()``) to
    ``subprocess.run``/``Popen``/..., ``boto3.client`` or a client ``*Api``.
    """
    bad: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        if "/spark/scripts/" in path.as_posix():
            continue  # run inside Spark pods, never in the CLI process
        rel = path.relative_to(SRC).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        bad.extend(f"{rel}:{line} {what}" for line, what in _bypasses(tree))
    assert bad == [], "entry points the recording fixture cannot see:\n" + "\n".join(bad)


@pytest.mark.parametrize(
    "snippet",
    [
        "import subprocess\nRUN = subprocess.run\n",
        "import subprocess\ndef f(runner=subprocess.run):\n    pass\n",
        "import os as _os\ndef f():\n    _os.system('x')\n",
        "import os\ndef f():\n    os.posix_spawn('x', [], {})\n",
        "import kubernetes.client.api.core_v1_api\n",
        "import kubernetes\ndef f():\n    kubernetes.client.api.CoreV1Api()\n",
        "from kubernetes import client\nCORE = client.CoreV1Api()\n",
        "from kubernetes import watch\n",
        "import boto3\ndef f():\n    boto3.session.Session()\n",
        "import botocore.session\n",
        "def f(api):\n    api.api_client.call_api('/x', 'DELETE')\n",
        "from subprocess import run\n",
        "from kubernetes.stream import stream\n",
    ],
)
def test_bypass_guard_catches(snippet):
    assert _bypasses(ast.parse(snippet)), snippet


def test_bypass_guard_allows_call_time_use():
    ok = (
        "import subprocess\nimport boto3\nfrom kubernetes import client\n"
        "def f():\n    from kubernetes.stream import stream\n"
        "    subprocess.run(['x'])\n    boto3.client('s3')\n    client.CoreV1Api()\n"
    )
    assert _bypasses(ast.parse(ok)) == []


def test_own_secretclass_names_match_the_templates():
    """The own-SecretClass rule must track what deploy really creates."""
    templates = SRC / "templates"
    found: set[str] = set()
    for path in templates.rglob("*.j2"):
        found |= set(re.findall(r"lakebench-s3-[a-z-]+?-\{\{ namespace \}\}", path.read_text()))
    names = {n.replace("{{ namespace }}", NS) for n in found}
    assert names and names <= own_secretclass_names(NS), names


# ---------------------------------------------------------------------------
# Real Lakebench code under the recorder
# ---------------------------------------------------------------------------


_LEGACY_SECRETCLASSES = ("lakebench-s3-credentials-class", "lakebench-s3-ca-cert-class")


def _destroy_under_recorder(rec: K8sRecorder) -> list:
    from unittest.mock import MagicMock, patch

    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict
    from lakebench.k8s.client import K8sClient
    from tests.conftest import make_config

    cfg = make_config(name=NS)
    rec.for_config(cfg)
    rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
    rec.add_spark_operator(watched=[NS, "u02"])
    rec.add_stackable()
    for name in (*_LEGACY_SECRETCLASSES, *sorted(own_secretclass_names(NS))):
        rec.add("secretclasses", {"metadata": {"name": name}})
    engine = MagicMock()
    engine.config = cfg
    engine.k8s = K8sClient(namespace=NS)  # real client, fake API classes
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name=NS,
        expected_deployment=NS,
        found_deployment=NS,
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=match),
    ):
        return destroy_all(engine, clean_buckets=False)


def test_real_destroy_is_recorded_and_scoped():
    """Unmodified destroy runs end to end against the fake.

    It reaches the watch-list upgrade and the namespace delete (both inside
    the lease), its own deletes are own, and the operator ends up watching
    only the other deployment.
    """
    from lakebench.spark import SparkOperatorManager

    with recording() as rec:
        results = _destroy_under_recorder(rec)
        assert {r.component: r.status.value for r in results}["namespace"] == "success"
        rec.assert_recorded(api="helm", verb="upgrade", name="spark-operator", lease_held=True)
        rec.assert_recorded(verb="delete", kind="namespaces", name=NS, scope=OWN, lease_held=True)
        assert ("namespaces", None, NS) not in rec.store
        mgr = SparkOperatorManager(namespace="spark-operator")
        assert mgr._get_active_namespaces() == ["u02"]
        own = [c for c in rec.mutations() if c.scope == OWN]
        assert {c.target for c in own} >= {
            "secrets/u01/lakebench-s3-credentials",
            "secretclasses/lakebench-s3-credentials-u01",
            "secretclasses/lakebench-s3-ca-cert-u01",
        }
        legacy = [c for c in rec.mutations() if c.name in _LEGACY_SECRETCLASSES]
        assert [c.scope for c in legacy] == [SHARED, SHARED]
        # Foreign deletes whatever the lease says; SD-11's test names them in
        # allow_delete once they run inside the lease.
        foreign = {v.call.name for v in rec.violations() if v.reason == REASON_FOREIGN_DELETE}
        assert foreign == set(_LEGACY_SECRETCLASSES)


@pytest.mark.xfail(
    strict=True,
    reason="SAF-4 defect at integrate 46cc3f4: destroy deletes the legacy "
    "SecretClasses without the cluster lease (destroy.py:2952-3009). SD-11 "
    "moves the cleanup into the lease; this then passes and the marker goes.",
)
def test_real_destroy_legacy_secretclass_delete_is_leased():
    with recording() as rec:
        _destroy_under_recorder(rec)
        legacy = [c for c in rec.mutations() if c.name in _LEGACY_SECRETCLASSES]
        assert legacy and all(c.lease_held for c in legacy), [c.describe() for c in legacy]


# ---------------------------------------------------------------------------
# Review round 1: false negatives found by the adversarial reviewers
# ---------------------------------------------------------------------------


class TestReviewFalseNegatives:
    def test_every_target_of_a_command_is_classified(self, tmp_path):
        from lakebench.k8s._pinned import pinned_kubectl

        crb = tmp_path / "crb.yaml"
        crb.write_text("kind: ClusterRoleBinding\nmetadata:\n  name: crb\n")
        own = tmp_path / "own.yaml"
        own.write_text(f"kind: ConfigMap\nmetadata:\n  name: x\n  namespace: {NS}\n")
        with recording(NS) as rec:
            pinned_kubectl("c", ["delete", "ns", NS, "u02"])
            pinned_kubectl("c", ["delete", "cm", "x", "-n", NS, "--namespace", "u02"])
            pinned_kubectl("c", ["apply", "-f", str(own), "-f", str(crb)])
            pinned_kubectl("c", ["delete", "pvc,secret", "--all", "-n", NS])
            assert [(c.target, c.scope) for c in rec.calls] == [
                ("namespaces/u01", OWN),
                ("namespaces/u02", SHARED),
                ("configmaps/u02/x", SHARED),  # the last -n wins, as in kubectl
                ("configmaps/u01/x", OWN),
                ("clusterrolebindings/crb", SHARED),
                ("persistentvolumeclaims/u01/*", OWN),
                ("secrets/u01/*", OWN),
            ]

    def test_wrappers_are_unwrapped(self):
        with recording(NS) as rec:
            subprocess.run(["sh", "-c", "kubectl delete ns u02 && echo done"])
            subprocess.run(["env", "A=1", "kubectl", "delete", "ns", "u02"])
            subprocess.run(["timeout", "30", "helm", "upgrade", "spark-operator", "c"])
            subprocess.run(["kubectl-1.30", "delete", "ns", "u02"])
            assert [c.target for c in rec.mutations()][:4] == [
                "namespaces/u02",
                "namespaces/u02",
                "releases/default/spark-operator",
                "namespaces/u02",
            ]
            assert _reasons(rec).count(REASON_FOREIGN_DELETE) == 3

    def test_child_lakebench_and_unknown_tools_are_violations(self):
        import sys

        with recording(NS) as rec:
            subprocess.run([sys.executable, "-m", "lakebench", "destroy", "x.yaml", "--force"])
            subprocess.run(["aws", "s3", "rm", "s3://u02-bronze", "--recursive"])
            assert set(_reasons(rec)) == {REASON_CHILD, REASON_UNKNOWN_TOOL}
            with pytest.raises(AssertionError, match="expected no cluster calls"):
                rec.assert_no_calls()

    def test_unknown_verb_fails_closed(self):
        with recording(NS) as rec:
            rec.add_namespace("u02")
            from lakebench.deploy.cluster_lock import cluster_lock

            with cluster_lock(_core(), timeout=0):
                subprocess.run(["kubectl", "--vv", "6", "delete", "ns", "u02"])
            assert REASON_FOREIGN_DELETE in _reasons(rec)

    def test_dry_run_false_is_a_real_run(self):
        with recording(NS) as rec:
            subprocess.run(["kubectl", "delete", "ns", "u02", "--dry-run=false"])
            subprocess.run(["kubectl", "delete", "ns", "u02", "--dry-run=none"])
            assert [c.mutating for c in rec.calls] == [True, True]

    def test_oc_remove_is_a_delete(self):
        from lakebench.deploy.cluster_lock import cluster_lock

        with recording(NS) as rec:
            with cluster_lock(_core(), timeout=0):
                subprocess.run(
                    [
                        "oc",
                        "adm",
                        "policy",
                        "remove-scc-from-user",
                        "anyuid",
                        "-z",
                        "s",
                        "-n",
                        "u02",
                    ]
                )
            assert REASON_FOREIGN_DELETE in _reasons(rec)

    def test_a_later_patch_is_detected(self):
        from unittest.mock import patch

        import lakebench.k8s._pinned as pinned

        with recording(NS) as rec:
            _core().list_namespace()
            with patch("subprocess.run"):
                subprocess.run(["kubectl", "delete", "ns", "u02"])  # unrecorded
            with patch.object(pinned, "subprocess"):
                pass
            assert _reasons(rec) == [REASON_PATCHED_OVER, REASON_PATCHED_OVER]
        with recording(NS) as rec:
            _core().list_namespace()
            # Its own context, undone before the recorder's: a test-level
            # monkeypatch here would restore the recorder's fake after it.
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(subprocess, "run", lambda *a, **k: None)
                with pytest.raises(AssertionError, match="another patch replaced"):
                    rec.assert_clean()

    def test_recording_restores_every_entry_point(self):
        import unittest.mock as um

        import boto3
        import kubernetes.client as kc
        import kubernetes.stream as kstream

        def entry_points():
            return (
                subprocess.run,
                subprocess.Popen,
                boto3.client,
                kc.CoreV1Api,
                kstream.stream,
                um._patch.__enter__,
            )

        before = entry_points()
        with recording(NS):
            pass
        assert entry_points() == before

    def test_recording_nothing_is_not_clean(self):
        with recording(NS) as rec:
            with pytest.raises(AssertionError, match="no cluster call was recorded"):
                rec.assert_clean()
            rec.assert_no_calls()
            rec.assert_clean()

    def test_assert_recorded(self):
        from lakebench.k8s._pinned import pinned_helm

        with recording(NS) as rec:
            pinned_helm("c", ["status", "x", "-n", "y"])
            with pytest.raises(AssertionError, match="no call matched"):
                rec.assert_recorded(api="helm", verb="upgrade")
            assert rec.assert_recorded(api="helm", verb="status")

    def test_lease_takeover_is_flagged(self, monkeypatch):
        """A cluster_lock regression that steals a live lease fails the oracle."""
        from lakebench.deploy import cluster_lock as cl
        from lakebench.k8s._pinned import pinned_helm

        with recording(NS) as rec:
            rec.seed_lease()
            monkeypatch.setattr(cl.LeaseState, "is_expired", lambda self, now_epoch=None: True)
            with cl.cluster_lock(_core(), timeout=0):
                pinned_helm("c", ["upgrade", "spark-operator", "chart", "-n", "spark-operator"])
            assert REASON_LEASE_TAKEN in _reasons(rec)

    def test_force_release_of_live_lease_is_flagged_unless_allowed(self):
        from lakebench.deploy.cluster_lock import (
            LOCK_CONFIGMAP_NAME,
            LOCK_NAMESPACE,
            force_release_cluster_lock,
        )

        with recording(NS) as rec:
            rec.seed_lease()
            force_release_cluster_lock(_core(), expired_only=False)
            assert _reasons(rec) == [REASON_LEASE_TAKEN]
        target = f"configmaps/{LOCK_NAMESPACE}/{LOCK_CONFIGMAP_NAME}"
        with recording(NS, allow_delete=[target]) as rec:
            rec.seed_lease()
            force_release_cluster_lock(_core(), expired_only=False)
            assert rec.violations() == []

    def test_lease_is_per_thread(self):
        import threading

        from lakebench.deploy.cluster_lock import cluster_lock
        from lakebench.k8s._pinned import pinned_helm

        with recording(NS) as rec:
            inside = threading.Event()
            done = threading.Event()

            def holder():
                with cluster_lock(_core(), timeout=0):
                    inside.set()
                    done.wait(5)

            t = threading.Thread(target=holder)
            t.start()
            assert inside.wait(5)
            pinned_helm("c", ["upgrade", "spark-operator", "chart", "-n", "spark-operator"])
            done.set()
            t.join(5)
            assert REASON_UNLEASED in _reasons(rec)

    def test_bucket_ownership_follows_tags_annotations_and_creation(self):
        from lakebench.deploy.ownership import ANNOTATION_ADOPTED_EMPTY_BUCKETS
        from lakebench.s3.client import S3Client

        names = ["u01-bronze", "u01-silver", "u01-gold", "u01-legacy", "u01-adopted"]
        with recording(NS, buckets=names) as rec:
            rec.add_namespace(NS, annotations={ANNOTATION_ADOPTED_EMPTY_BUCKETS: "u01-adopted"})
            rec.add_bucket("u01-bronze", ["k"], tags={"lakebench.deployment": "other"})
            rec.add_bucket("u01-silver", ["k"], tags={"lakebench.deployment": NS})
            rec.add_bucket("u01-legacy", ["k"])  # no tag, no annotation: not provably ours
            rec.add_bucket("u01-adopted", ["k"])
            raw = S3Client(
                endpoint="http://10.0.1.50:80", access_key="k", secret_key="s"
            ).raw_client
            raw.delete_object(Bucket="u01-bronze", Key="k")  # foreign tag
            raw.delete_object(Bucket="u01-silver", Key="k")  # ours: may empty
            raw.delete_bucket(Bucket="u01-silver")  # ours, but not created by us
            raw.create_bucket(Bucket="u01-gold")
            raw.delete_bucket(Bucket="u01-gold")  # created here
            raw.delete_object(Bucket="u01-legacy", Key="k")
            raw.delete_object(Bucket="u01-adopted", Key="k")
            tag = {"TagSet": [{"Key": "lakebench.deployment", "Value": NS}]}
            raw.put_bucket_tagging(Bucket="u01-legacy", Tagging=tag)  # deploy adopting it
            raw.put_bucket_tagging(Bucket="u01-bronze", Tagging=tag)  # overwriting B's tag
            assert [(v.call.name, v.reason) for v in rec.violations()] == [
                ("u01-bronze/k", REASON_FOREIGN_DELETE),
                ("u01-silver", REASON_NOT_CREATED),
                ("u01-legacy/k", REASON_FOREIGN_DELETE),
                ("u01-bronze", REASON_FOREIGN_BUCKET),
            ]

    def test_s3_tagging_unsupported_and_faults(self):
        from botocore.exceptions import ClientError

        with recording(NS, buckets=["u01-bronze"]) as rec:
            rec.add_bucket("u01-bronze")
            rec.s3_tagging = False
            from lakebench.s3.client import S3Client

            raw = S3Client(
                endpoint="http://10.0.1.50:80", access_key="k", secret_key="s"
            ).raw_client
            with pytest.raises(ClientError, match="NotImplemented"):
                raw.get_bucket_tagging(Bucket="u01-bronze")
            rec.fail_s3("head_bucket", "AccessDenied", status=403, times=1)
            with pytest.raises(ClientError, match="AccessDenied"):
                raw.head_bucket(Bucket="u01-bronze")
            raw.head_bucket(Bucket="u01-bronze")

    def test_shell_scripts_are_split_or_fail_closed(self):
        with recording(NS) as rec:
            subprocess.run(["sh", "-c", "kubectl get ns x; kubectl delete ns u02"])
            subprocess.run("kubectl get ns x&&kubectl delete ns u03", shell=True)
            subprocess.run(["bash", "-c", "kubectl get ns x\nkubectl delete ns u04"])
            subprocess.run(["sh", "-c", "kubectl delete ns $(cat f)"])
            deleted = [c.name for c in rec.calls if c.verb == "delete"]
            assert deleted == ["u02", "u03", "u04"]
            assert REASON_UNKNOWN_TOOL in _reasons(rec)  # the substitution

    def test_cli_lease_mutation_and_forced_deletes(self, tmp_path):
        from lakebench.deploy.cluster_lock import cluster_lock

        cm = tmp_path / "cm.yaml"
        cm.write_text("kind: ConfigMap\nmetadata:\n  name: x\n  namespace: u02\n")
        with recording(NS) as rec:
            rec.seed_lease()
            subprocess.run(
                ["kubectl", "delete", "cm", "lakebench-cluster-lock", "-n", "lakebench-system"]
            )
            assert _reasons(rec) == [REASON_LEASE_CLI]
        with recording(NS) as rec:
            with cluster_lock(_core(), timeout=0):
                subprocess.run(["kubectl", "replace", "--force", "-f", str(cm)])
                subprocess.run(["kubectl", "apply", "--prune", "-A", "-l", "a=b", "-f", str(cm)])
                subprocess.run(["kubectl", "drain", "node-1"])
            assert _reasons(rec) == [REASON_FOREIGN_DELETE] * 3

    def test_takeover_by_another_thread_is_flagged(self):
        import threading

        from lakebench.deploy import cluster_lock as cl

        with recording(NS) as rec:
            got = threading.Event()
            done = threading.Event()

            def holder():
                with cl.cluster_lock(_core(), timeout=0):
                    got.set()
                    done.wait(5)

            t = threading.Thread(target=holder)
            t.start()
            assert got.wait(5)
            cm = _core().read_namespaced_config_map(cl.LOCK_CONFIGMAP_NAME, cl.LOCK_NAMESPACE)
            _core().replace_namespaced_config_map(cl.LOCK_CONFIGMAP_NAME, cl.LOCK_NAMESPACE, cm)
            done.set()
            t.join(5)
            assert REASON_LEASE_TAKEN in _reasons(rec)

    def test_kubectl_get_answers_every_name(self):
        with recording(NS) as rec:
            rec.add_namespace("a")
            rec.add_namespace("b")
            r = subprocess.run(
                ["kubectl", "get", "ns", "a", "b", "-o", "json"], capture_output=True, text=True
            )
            import json

            assert [i["metadata"]["name"] for i in json.loads(r.stdout)["items"]] == ["a", "b"]

    def test_custom_kinds_follow_their_crd(self):
        """A namespaced call on a cluster-scoped CRD 404s and is never own."""
        from kubernetes import client
        from kubernetes.client.exceptions import ApiException

        from lakebench.k8s.client import K8sClient

        with recording(NS) as rec:
            rec.add_namespace(NS)
            rec.add_stackable()
            api = client.CustomObjectsApi()
            body = {"metadata": {"name": "lakebench-s3-credentials-u02"}}
            with pytest.raises(ApiException) as e:
                api.create_namespaced_custom_object(
                    "secrets.stackable.tech", "v1alpha1", NS, "secretclasses", body
                )
            assert e.value.status == 404
            with pytest.raises(ApiException):
                api.create_namespaced_custom_object(
                    "x.io", "v1", NS, "authenticationclasses", {"metadata": {"name": "a"}}
                )
            assert [c.scope for c in rec.mutations()] == [SHARED, SHARED]
            # The real client reads the seeded CRD and takes the cluster path.
            k8s = K8sClient(namespace=NS)
            assert k8s._is_cluster_scoped_crd("secrets.stackable.tech", "v1alpha1", "SecretClass")

    def test_unscripted_read_is_a_violation(self):
        from lakebench.k8s._pinned import pinned_kubectl

        with recording(NS) as rec:
            rec.add_spark_operator(watched=[NS])
            r = pinned_kubectl("c", ["describe", "pod", "x", "-n", NS], capture_output=True)
            assert r.returncode == 1
            pinned_kubectl(
                "c",
                ["get", "pods", "-n", "spark-operator", "-o", "jsonpath={range .items[*]}{end}"],
            )
            assert _reasons(rec) == [REASON_UNSCRIPTED, REASON_UNSCRIPTED]

    def test_helm_upgrade_rewrites_the_operator_watch_list(self):
        from lakebench.spark import SparkOperatorManager

        with recording(NS) as rec:
            rec.add_spark_operator(watched=[NS, "u02"])
            mgr = SparkOperatorManager(namespace="spark-operator", kube_context="c")
            assert mgr._get_watched_namespaces() == [NS, "u02"]
            subprocess.run(
                [
                    "helm",
                    "upgrade",
                    "spark-operator",
                    "spark-operator/spark-operator",
                    "-n",
                    "spark-operator",
                    "--reuse-values",
                    "--set",
                    "spark.jobNamespaces={u02}",
                ]
            )
            assert mgr._get_watched_namespaces() == ["u02"]
            assert mgr._get_active_namespaces() == ["u02"]

    def test_api_dry_run_is_not_a_mutation(self):
        with recording(NS) as rec:
            rec.add_namespace("u02")
            _core().delete_namespace("u02", dry_run="All")
            assert rec.mutations() == [] and ("namespaces", None, "u02") in rec.store

    def test_set_based_label_selector(self):
        with recording(NS) as rec:
            rec.add_spark_operator()
            pods = _core().list_namespaced_pod(
                "spark-operator", label_selector="app.kubernetes.io/component in (controller)"
            )
            assert [p.metadata.name for p in pods.items] == ["spark-operator-controller-0"]

    def test_openshift_seed_is_detected_by_real_code(self):
        from lakebench.k8s.client import K8sClient
        from lakebench.k8s.security import PlatformType, SecurityVerifier

        with recording(NS) as rec:
            rec.add_openshift()
            sv = SecurityVerifier(K8sClient(namespace=NS))
            assert sv.detect_platform() == PlatformType.OPENSHIFT
            assert sv.get_platform_version() == "4.16.0"

    def test_dict_seed_that_does_not_validate_is_refused(self):
        rec = K8sRecorder(NS)
        with pytest.raises(ValueError, match="does not validate"):
            rec.add("customresourcedefinitions", {"metadata": {"name": "x.io"}})
