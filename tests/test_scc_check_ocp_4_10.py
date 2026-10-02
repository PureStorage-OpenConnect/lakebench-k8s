"""LB-093: `_check_scc_assignment` must verify the OCP 4.10+ mechanism
(namespaced ``system:openshift:scc:<scc>`` RoleBinding subjects), not
the legacy cluster-scoped ``.users`` array alone.

DEP-4 moved both reads from ``oc`` to the API (the RBAC API for the
RoleBinding, the ``security.openshift.io/v1`` custom object for the legacy
``.users``), so verification works without ``oc`` on PATH. The cases are
the LB-093 ones.
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest
from kubernetes.client.rest import ApiException

from lakebench.k8s.security import SecurityVerifier


@pytest.fixture(autouse=True)
def _no_oc(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("verification must not call oc")

    monkeypatch.setattr("lakebench.k8s.security.pinned_oc", refuse, raising=False)


def _check(rb_subjects=None, rb_error=None, users=None, scc_error=None):
    """Run ``_check_scc_assignment`` against a fake RBAC and SCC API.

    ``rb_subjects=None`` means the RoleBinding does not exist (404).
    """
    rbac = MagicMock()
    rbac.api_client.sanitize_for_serialization.side_effect = lambda o: o
    if rb_error is not None:
        rbac.read_namespaced_role_binding.side_effect = rb_error
    elif rb_subjects is None:
        rbac.read_namespaced_role_binding.side_effect = ApiException(status=404)
    else:
        rbac.read_namespaced_role_binding.return_value = {"subjects": rb_subjects}
    custom = MagicMock()
    if scc_error is not None:
        custom.get_cluster_custom_object.side_effect = scc_error
    else:
        custom.get_cluster_custom_object.return_value = {"users": users or []}
    with (
        patch("kubernetes.client.RbacAuthorizationV1Api", return_value=rbac),
        patch("kubernetes.client.CustomObjectsApi", return_value=custom),
    ):
        return SecurityVerifier(k8s=Mock())._check_scc_assignment(
            "anyuid", "lakebench-spark-runner", "myns"
        )


def _sa(name="lakebench-spark-runner", ns="myns"):
    return {"kind": "ServiceAccount", "name": name, "namespace": ns}


class TestCheckSccAssignmentReadsRoleBindingFirst:
    def test_ocp_4_10_grant_via_rolebinding(self):
        status = _check(rb_subjects=[_sa()])
        assert status.assigned is True
        assert "RoleBinding" in status.message

    def test_pre_ocp_4_10_grant_via_users_field(self):
        status = _check(users=["system:serviceaccount:myns:lakebench-spark-runner"])
        assert status.assigned is True
        assert "legacy" in status.message.lower()

    def test_unassigned_when_neither_mechanism_grants(self):
        status = _check()
        assert status.assigned is False
        assert "not assigned" in status.message

    def test_rolebinding_present_but_different_ns_is_not_a_grant(self):
        assert _check(rb_subjects=[_sa(ns="otherns")]).assigned is False

    def test_read_error_reports_cannot_check(self):
        status = _check(rb_error=ApiException(status=403, reason="Forbidden"))
        assert status.assigned is False
        assert status.message.startswith("Error checking SCC")


class TestDeadHelperRemoved:
    def test_scc_has_user_no_longer_exists(self):
        """A legacy `.users`-only helper by this name recreates LB-088."""
        assert not hasattr(SecurityVerifier, "_scc_has_user")

    def test_oc_retry_loop_is_gone(self):
        """DEP-4 replaced the `oc adm policy` retry loop with the RBAC API."""
        assert not hasattr(SecurityVerifier, "_add_scc")


class TestScvUsersFieldExactMatch:
    """The legacy `.users` fallback must not substring-match (LB-093)."""

    def test_exact_match_wins(self):
        users = [
            "system:serviceaccount:myns:lakebench-spark-runner",
            "system:serviceaccount:otherns:some-other-sa",
        ]
        assert _check(users=users).assigned is True

    def test_no_substring_false_positive_on_suffixed_sa(self):
        users = ["system:serviceaccount:myns:lakebench-spark-runner-v2"]
        assert _check(users=users).assigned is False

    def test_empty_users_returns_false(self):
        assert _check(users=[]).assigned is False

    def test_scc_read_error_returns_false(self):
        assert _check(scc_error=ApiException(status=404)).assigned is False
