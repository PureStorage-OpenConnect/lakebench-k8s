"""DEP-4: the anyuid SCC grant goes through the Kubernetes API.

``ensure_scc_rolebinding`` asks a LocalSubjectAccessReview whether the
ServiceAccount may already use the SCC, otherwise makes the call ``oc adm
policy add-scc-to-user`` makes on OCP 4.10+ (the namespaced RoleBinding
``system:openshift:scc:<scc>`` bound to the ClusterRole of the same name),
then reviews again. A deploy needs no ``oc``.

Nothing here reaches a cluster or runs ``oc``: the APIs are fakes.
"""

from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from lakebench.deploy.engine import DeploymentStatus
from lakebench.k8s import PlatformType
from lakebench.k8s.security import SCCGrantError, SecurityVerifier, ensure_scc_rolebinding

ROLE = "system:openshift:scc:anyuid"
NS = "lb-ns"
SA = "lakebench-spark-runner"


def _sub(name: str, ns: str = NS) -> dict:
    return {"kind": "ServiceAccount", "name": name, "namespace": ns}


def _rb(subjects: list[dict], rv: str = "7", ref: str = ROLE) -> dict:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": ROLE, "namespace": NS, "resourceVersion": rv},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": ref},
        "subjects": subjects,
    }


class FakeRbac:
    """RBAC API with optimistic concurrency on resourceVersion."""

    def __init__(
        self,
        rb: dict | None = None,
        create_status: int | None = None,
        racer: dict | None = None,
        always_conflict: bool = False,
    ):
        self.rb = copy.deepcopy(rb)
        self.create_status = create_status
        self.racer = racer
        self.always_conflict = always_conflict
        self.creates: list[dict] = []
        self.replaces: list[dict] = []
        self.api_client = SimpleNamespace(sanitize_for_serialization=copy.deepcopy)

    def read_namespaced_role_binding(self, name, ns):
        assert name == ROLE
        if self.rb is None:
            raise ApiException(status=404)
        return copy.deepcopy(self.rb)

    def create_namespaced_role_binding(self, ns, body):
        if self.create_status is not None:
            raise ApiException(status=self.create_status, reason="Forbidden")
        if self.rb is not None:
            raise ApiException(status=409)
        self.creates.append(copy.deepcopy(body))
        self.rb = {**copy.deepcopy(body), "metadata": {**body["metadata"], "resourceVersion": "1"}}

    def replace_namespaced_role_binding(self, name, ns, body):
        if self.racer is not None:
            self.rb = copy.deepcopy(self.racer)
            self.racer = None
        stale = body["metadata"]["resourceVersion"] != self.rb["metadata"]["resourceVersion"]
        if self.always_conflict or stale:
            raise ApiException(status=409)
        self.replaces.append(copy.deepcopy(body))
        rv = str(int(self.rb["metadata"]["resourceVersion"]) + 1)
        self.rb = {**copy.deepcopy(body), "metadata": {**body["metadata"], "resourceVersion": rv}}


class FakeAuthz:
    """LocalSubjectAccessReview: allowed once ``rbac`` binds the SA, unless
    ``allowed`` forces an answer or ``status`` makes the review fail."""

    def __init__(self, rbac: FakeRbac | None = None, allowed=None, status: int | None = None):
        self.rbac = rbac
        self.allowed = allowed
        self.status = status
        self.reviews: list[dict] = []

    def create_namespaced_local_subject_access_review(self, ns, body):
        if self.status is not None:
            raise ApiException(status=self.status)
        self.reviews.append(body)
        if self.allowed is not None:
            ok = self.allowed
        else:
            subs = (self.rbac.rb or {}).get("subjects", []) if self.rbac else []
            ok = any(s.get("name") == SA and s.get("namespace") == NS for s in subs)
        return SimpleNamespace(status=SimpleNamespace(allowed=ok))


@pytest.fixture(autouse=True)
def _no_oc(monkeypatch):
    """Any ``oc`` call fails the test instead of reaching a cluster."""

    def refuse(*a, **k):
        raise AssertionError(f"oc must not be called: {a}")

    monkeypatch.setattr("lakebench.k8s.security.pinned_oc", refuse, raising=False)
    monkeypatch.setattr("lakebench.k8s._pinned.pinned_oc", refuse)


def _grant(api: FakeRbac, authz: FakeAuthz | None = None):
    ensure_scc_rolebinding(api, NS, SA, authz_api=authz or FakeAuthz(api))


def test_review_asks_what_scc_admission_asks():
    api = FakeRbac()
    authz = FakeAuthz(api)
    _grant(api, authz)
    spec = authz.reviews[0]["spec"]
    assert spec["user"] == f"system:serviceaccount:{NS}:{SA}"
    assert spec["groups"] == ["system:serviceaccounts", f"system:serviceaccounts:{NS}"]
    assert spec["resourceAttributes"] == {
        "namespace": NS,
        "verb": "use",
        "group": "security.openshift.io",
        "resource": "securitycontextconstraints",
        "name": "anyuid",
    }


def test_existing_grant_writes_nothing():
    """An admin's grant (any mechanism) is enough: no RoleBinding write."""
    api = FakeRbac()
    _grant(api, FakeAuthz(allowed=True))
    assert api.creates == [] and api.replaces == []


def test_creates_the_rolebinding_when_absent():
    api = FakeRbac()
    _grant(api)
    [body] = api.creates
    assert body["metadata"] == {"name": ROLE, "namespace": NS}
    assert body["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": ROLE,
    }
    assert body["subjects"] == [_sub(SA)]


def test_scc_rolebinding_merge():
    """An existing binding keeps its subjects; ours is added under the
    read's resourceVersion (compare-and-swap)."""
    api = FakeRbac(rb=_rb([_sub("lakebench-postgres")], rv="7"))
    _grant(api)
    [body] = api.replaces
    assert body["metadata"]["resourceVersion"] == "7"
    assert body["subjects"] == [_sub("lakebench-postgres"), _sub(SA)]


def test_no_write_when_already_bound():
    api = FakeRbac(rb=_rb([_sub(SA)]))
    _grant(api, FakeAuthz(status=403))  # review unavailable: the binding decides
    assert api.creates == [] and api.replaces == []


def test_same_sa_in_another_namespace_is_not_a_grant():
    api = FakeRbac(rb=_rb([_sub(SA, ns="other")]))
    _grant(api)
    assert api.replaces[0]["subjects"] == [_sub(SA, ns="other"), _sub(SA)]


def test_conflict_rereads_and_keeps_the_racers_subject():
    racer = _rb([_sub("lakebench-postgres")], rv="8")
    api = FakeRbac(rb=_rb([], rv="7"), racer=racer)
    _grant(api)
    assert api.rb["subjects"] == [_sub("lakebench-postgres"), _sub(SA)]


def test_persistent_conflict_fails():
    api = FakeRbac(rb=_rb([]), always_conflict=True)
    with pytest.raises(SCCGrantError, match="kept changing"):
        _grant(api)


def test_foreign_roleref_fails():
    api = FakeRbac(rb=_rb([], ref="system:openshift:scc:restricted"))
    with pytest.raises(SCCGrantError, match="roleRef"):
        _grant(api)


def test_forbidden_bind_fails_with_admin_command():
    api = FakeRbac(create_status=403)
    with pytest.raises(SCCGrantError) as ei:
        _grant(api)
    msg = str(ei.value)
    assert msg.startswith(f"cannot grant SCC anyuid to SA {SA} in namespace {NS}: Forbidden")
    assert f"oc adm policy add-scc-to-user anyuid -z {SA} -n {NS}" in msg


def test_binding_that_does_not_take_effect_fails(monkeypatch):
    """A binding to a missing ClusterRole (pre-4.10) is written but grants
    nothing; the second review catches it."""
    monkeypatch.setattr("lakebench.k8s.security.SCC_VERIFY_TIMEOUT_S", 0.0)
    api = FakeRbac()
    with pytest.raises(SCCGrantError, match="still may not use SCC anyuid"):
        _grant(api, FakeAuthz(allowed=False))


def test_second_review_waits_for_the_binding_to_propagate(monkeypatch):
    """A fresh binding can reach the authorizer cache a moment late."""
    monkeypatch.setattr("lakebench.k8s.security.time.sleep", lambda s: None)
    answers = iter([False, False, False, True])

    class Lagging(FakeAuthz):
        def create_namespaced_local_subject_access_review(self, ns, body):
            return SimpleNamespace(status=SimpleNamespace(allowed=next(answers)))

    api = FakeRbac()
    _grant(api, Lagging())
    assert api.creates


def test_review_connection_error_is_unknown_not_a_crash():
    from lakebench.k8s.security import _sa_can_use_scc

    authz = MagicMock()
    authz.create_namespaced_local_subject_access_review.side_effect = ConnectionError("reset")
    assert _sa_can_use_scc(authz, NS, SA, "anyuid") is None


def test_review_unavailable_still_grants():
    api = FakeRbac()
    _grant(api, FakeAuthz(status=403))
    assert api.creates


def test_review_dict_response_is_read():
    from lakebench.k8s.security import _sa_can_use_scc

    authz = MagicMock()
    authz.create_namespaced_local_subject_access_review.return_value = {"status": {"allowed": True}}
    assert _sa_can_use_scc(authz, NS, SA, "anyuid") is True


# -- callers --------------------------------------------------------------------


def _rbac_engine():
    renderer = MagicMock()
    renderer.render.return_value = "kind: ServiceAccount\nmetadata:\n  name: x\n"
    cfg = MagicMock()
    cfg.get_namespace.return_value = NS
    cfg.images.spark = "apache/spark:4.0.1"
    return SimpleNamespace(
        config=cfg, k8s=MagicMock(), renderer=renderer, context={}, dry_run=False
    )


def _openshift(monkeypatch):
    monkeypatch.setattr(
        SecurityVerifier, "detect_platform", lambda self, strict=False: PlatformType.OPENSHIFT
    )


def test_scc_success_keeps_step_success(monkeypatch):
    from lakebench.modules.pipeline_engines.spark.rbac import RBACDeployer

    _openshift(monkeypatch)
    api = FakeRbac()
    with (
        patch("kubernetes.client.RbacAuthorizationV1Api", return_value=api),
        patch("kubernetes.client.AuthorizationV1Api", return_value=FakeAuthz(api)),
    ):
        result = RBACDeployer(_rbac_engine()).deploy()
    assert result.status is DeploymentStatus.SUCCESS
    assert api.rb["subjects"] == [_sub(SA)]


def test_rbac_failure_message_is_the_grant_error(monkeypatch):
    """The step's message is the grant's one line, not a generic wrapper."""
    from lakebench.modules.pipeline_engines.spark.rbac import RBACDeployer

    _openshift(monkeypatch)
    api = FakeRbac(create_status=403)
    with (
        patch("kubernetes.client.RbacAuthorizationV1Api", return_value=api),
        patch("kubernetes.client.AuthorizationV1Api", return_value=FakeAuthz(api)),
    ):
        result = RBACDeployer(_rbac_engine()).deploy()
    assert result.status is DeploymentStatus.FAILED
    assert result.message.startswith(f"cannot grant SCC anyuid to SA {SA} in namespace {NS}")


def test_failed_platform_detection_fails_the_rbac_step():
    """Discovery failing must not read as vanilla and skip the grant."""
    from lakebench.modules.pipeline_engines.spark.rbac import RBACDeployer

    apis = MagicMock()
    apis.get_api_versions.side_effect = RuntimeError("apiserver 503")
    with patch("kubernetes.client.ApisApi", return_value=apis):
        result = RBACDeployer(_rbac_engine()).deploy()
    assert result.status is DeploymentStatus.FAILED
    assert "apiserver 503" in result.message


@pytest.mark.parametrize(
    "groups,expected",
    [
        (["apps", "security.openshift.io"], PlatformType.OPENSHIFT),
        (["route.openshift.io"], PlatformType.VANILLA),
    ],
)
def test_platform_is_keyed_on_the_scc_api_group(groups, expected, monkeypatch):
    monkeypatch.setattr(SecurityVerifier, "_detect_openshift_version", lambda self: None)
    apis = MagicMock()
    apis.get_api_versions.return_value = SimpleNamespace(
        groups=[SimpleNamespace(name=g) for g in groups]
    )
    with patch("kubernetes.client.ApisApi", return_value=apis):
        assert SecurityVerifier(MagicMock()).detect_platform(strict=True) is expected


def test_operator_scc_strict_at_install_lenient_on_watch_edits(monkeypatch, caplog):
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    mgr = SparkOperatorManager.__new__(SparkOperatorManager)
    mgr.namespace = "spark-operator"

    def refused(rbac, ns, sa, scc="anyuid", **kw):
        raise SCCGrantError(f"cannot grant SCC {scc} to SA {sa} in namespace {ns}: Forbidden")

    monkeypatch.setattr("lakebench.k8s.security.ensure_scc_rolebinding", refused)
    with patch("kubernetes.client.RbacAuthorizationV1Api", return_value=FakeRbac()):
        mgr._assign_openshift_scc()  # a deploy's watch-list edit: logged only
        assert "spark-operator-controller" in caplog.text
        with pytest.raises(SCCGrantError):
            mgr._assign_openshift_scc(strict=True)
