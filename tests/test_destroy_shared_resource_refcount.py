"""Destroy's refcount for the shared legacy SecretClasses, on the real path (DEP-7).

The two legacy fixed-name SecretClasses (``lakebench-s3-credentials-class``,
``lakebench-s3-ca-cert-class``) are cluster-scoped and shared by every
pre-PR-2 deployment. Deleting them while another lakebench namespace still
uses them has crashed other users' Hive Metastore pods. Destroy deletes them
only when no other lakebench namespace is left.

These tests drive ``destroy_all`` itself under the recording fixture, so an
inverted predicate in the code destroy really runs fails them. The earlier
tests exercised two helpers that ``destroy_all`` never called.

Also here: destroy deleting the PostgreSQL claims, which carry no
labels.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.fixtures.recording_k8s import K8sRecorder, recording

NS = "u01"
LEGACY = ("lakebench-s3-credentials-class", "lakebench-s3-ca-cert-class")
OWN_SC = (f"lakebench-s3-credentials-{NS}", f"lakebench-s3-ca-cert-{NS}")


def _destroy(rec: K8sRecorder, *, create_namespace: bool = True) -> list:
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict
    from lakebench.k8s.client import K8sClient
    from tests.conftest import make_config

    cfg = make_config(name=NS)
    cfg.platform.kubernetes.create_namespace = create_namespace
    rec.for_config(cfg)
    rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
    rec.add_spark_operator(watched=[NS])
    rec.add_stackable()
    for name in (*LEGACY, *OWN_SC):
        rec.add("secretclasses", {"metadata": {"name": name}})
    engine = MagicMock()
    engine.config = cfg
    engine.k8s = K8sClient(namespace=NS)
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


def _legacy_deletes(rec: K8sRecorder) -> list[str]:
    return [
        c.name
        for c in rec.mutations()
        if c.verb == "delete" and c.kind == "secretclasses" and c.name in LEGACY
    ]


def _status(results: list, component: str) -> str:
    return {r.component: r.status.value for r in results}[component]


class TestLegacySecretClassRefcountInline:
    """``test_destroy_legacy_secretclass_refcount_inline``."""

    def test_deleted_when_only_this_namespace_exists(self):
        with recording(allow_delete=[f"secretclasses/{n}" for n in LEGACY]) as rec:
            results = _destroy(rec)
            assert _status(results, "rbac") == "success"
            assert sorted(_legacy_deletes(rec)) == sorted(LEGACY)
            # The deployment's own SecretClasses go too (proves the step ran).
            rec.assert_recorded(verb="delete", kind="secretclasses", name=OWN_SC[0])
            rec.assert_clean()  # the legacy deletes ran inside the lease (SAF-4)

    @pytest.mark.parametrize(
        "other",
        [
            {"annotations": {"lakebench.deployment/name": "u02"}},
            {"labels": {"app.kubernetes.io/managed-by": "lakebench"}},
            {"labels": {"app.kubernetes.io/name": "lakebench"}},
            {"labels": {"app.kubernetes.io/managed-by": "lakebench"}, "phase": "Terminating"},
        ],
        ids=["annotated", "managed-by-label", "name-label", "terminating"],
    )
    def test_kept_while_another_lakebench_namespace_exists(self, other):
        with recording() as rec:
            rec.add_namespace("u02", **other)
            results = _destroy(rec)
            assert _status(results, "rbac") == "success"
            rec.assert_recorded(verb="delete", kind="secretclasses", name=OWN_SC[0])
            assert _legacy_deletes(rec) == []
            assert ("secretclasses", None, LEGACY[0]) in rec.store
            rec.assert_clean()

    def test_unrelated_namespace_does_not_count(self):
        with recording(allow_delete=[f"secretclasses/{n}" for n in LEGACY]) as rec:
            rec.add_namespace("kube-system")
            rec.add_namespace("someone-else", labels={"app.kubernetes.io/managed-by": "helm"})
            rec.add_namespace(
                "empty-annotation",
                annotations={"lakebench.deployment/name": ""},
                labels={"app": "x"},
            )
            _destroy(rec)
            assert sorted(_legacy_deletes(rec)) == sorted(LEGACY)
            rec.assert_clean()

    def test_kept_when_the_namespace_list_fails(self):
        with recording() as rec:
            rec.fail(verb="list", kind="namespaces", status=500)
            results = _destroy(rec)
            rec.assert_recorded(verb="list", kind="namespaces")
            rec.assert_recorded(verb="delete", kind="secretclasses", name=OWN_SC[0])
            assert _status(results, "rbac") == "success"
            assert _legacy_deletes(rec) == []


class TestPostgresClaims:
    """The claim template has no labels; destroy finds the claims by name."""

    @staticmethod
    def _pvc(rec: K8sRecorder, name: str, labels: dict | None = None) -> None:
        rec.add(
            "persistentvolumeclaims",
            {
                "metadata": {"name": name, "labels": labels},
                "spec": {"accessModes": ["ReadWriteOnce"]},
            },
            namespace=NS,
        )

    def test_postgres_pvc_deleted_without_label(self):
        with recording(allow_delete=[f"secretclasses/{n}" for n in LEGACY]) as rec:
            self._pvc(rec, "data-lakebench-postgres-0")
            self._pvc(rec, "data-lakebench-postgres-1")
            # Not ours: another app's claim with the label the old selector
            # used, and look-alike names.
            self._pvc(rec, "pg-data", {"app.kubernetes.io/component": "postgres"})
            self._pvc(rec, "data-lakebench-postgres-0-backup")
            self._pvc(rec, "spark-scratch-1")
            results = _destroy(rec, create_namespace=False)
            assert _status(results, "postgres") == "success"
            left = sorted(name for kind, ns, name in rec.store if kind == "persistentvolumeclaims")
            assert left == ["data-lakebench-postgres-0-backup", "pg-data", "spark-scratch-1"]
            # The Postgres step deleted exactly its two claims.
            postgres_deletes = sorted(
                c.name
                for c in rec.mutations()
                if c.verb == "delete"
                and c.kind == "persistentvolumeclaims"
                and c.name.startswith("data-lakebench-postgres")
            )
            assert postgres_deletes == ["data-lakebench-postgres-0", "data-lakebench-postgres-1"]

    def test_claim_template_stays_unlabelled(self):
        """Labels added to volumeClaimTemplates would make a v1.7 deploy over a
        v1.6 StatefulSet fail: the API server refuses changes to that field."""
        import yaml

        from lakebench.deploy.engine import TemplateRenderer
        from tests.fixtures.functional_templates_helpers import _enrich_context, _make_engine

        ctx = _enrich_context(_make_engine())
        rendered = TemplateRenderer().render("postgres/statefulset.yaml.j2", ctx)
        (sts,) = [d for d in yaml.safe_load_all(rendered) if d and d["kind"] == "StatefulSet"]
        claims = sts["spec"]["volumeClaimTemplates"]
        assert [c["metadata"] for c in claims] == [{"name": "data"}]
