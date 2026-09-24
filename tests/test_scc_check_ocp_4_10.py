"""LB-093: `_check_scc_assignment` must verify the OCP 4.10+ mechanism
(namespaced ``system:openshift:scc:<scc>`` RoleBinding subjects), not
the legacy cluster-scoped ``.users`` array alone.

The LB-088 wave fixed this in ``_add_scc`` by adding
``_scc_binding_has_subject``. The parallel gap in the preflight check
went unfixed until LB-093 -- so ``verify_security()`` used to report
every SCC as "not assigned" on modern OCP even after a successful add.

Structure mirrors ``tests/test_scc_add_retry.py``.
"""

from __future__ import annotations

from subprocess import CompletedProcess
from unittest.mock import Mock, patch

from lakebench.k8s.security import SecurityVerifier


def _make(cmd_stdout="", cmd_stderr="", cmd_returncode=0):
    return CompletedProcess(
        args=[], returncode=cmd_returncode, stdout=cmd_stdout, stderr=cmd_stderr
    )


class TestCheckSccAssignmentReadsRoleBindingFirst:
    def test_ocp_4_10_grant_via_rolebinding(self):
        """On OCP 4.10+ `.users` stays empty; the grant lives on the
        namespaced RoleBinding. The verifier must find it."""
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                # subjects listed one per "ns/sa" token, space separated
                return _make(cmd_stdout="myns/lakebench-spark-runner ")
            if "scc" in cmd:
                # legacy .users is empty on modern OCP
                return _make(cmd_stdout="[]")
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is True
        assert "RoleBinding" in status.message

    def test_pre_ocp_4_10_grant_via_users_field(self):
        """The legacy `.users` fallback must still work when the
        RoleBinding does not exist. LB-093 follow-up: the fallback now
        parses tokens one per line and matches exactly."""
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                return _make(cmd_returncode=1, cmd_stderr="not found")
            if "scc" in cmd:
                return _make(cmd_stdout="system:serviceaccount:myns:lakebench-spark-runner\n")
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is True
        assert "legacy" in status.message.lower()

    def test_unassigned_when_neither_mechanism_grants(self):
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                return _make(cmd_returncode=1, cmd_stderr="not found")
            if "scc" in cmd:
                return _make(cmd_stdout="[]")
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is False
        assert "not assigned" in status.message

    def test_rolebinding_present_but_different_ns_is_not_a_grant(self):
        """A binding for the same SA in a different namespace must not
        satisfy the check -- namespace is part of the grant identity."""
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                # subject in otherns, not myns
                return _make(cmd_stdout="otherns/lakebench-spark-runner ")
            if "scc" in cmd:
                return _make(cmd_stdout="[]")
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is False

    def test_oc_missing_reports_actionable_message(self):
        verifier = SecurityVerifier(k8s=Mock())
        with patch("lakebench.k8s.security.subprocess.run", side_effect=FileNotFoundError("oc")):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is False
        assert "oc" in status.message


class TestDeadHelperRemoved:
    def test_scc_has_user_no_longer_exists(self):
        """LB-093 also deletes the dead `_scc_has_user` helper (no callers).
        Guard against it reappearing as a maintenance regression: if a
        future contributor re-adds a legacy `.users`-only helper by that
        name, they are almost certainly recreating the LB-088 bug.
        """
        assert not hasattr(SecurityVerifier, "_scc_has_user")


class TestScvUsersFieldExactMatch:
    """LB-093 follow-up: the legacy `.users` fallback must not substring-match.
    A subagent verifier flagged that `user in result.stdout` would return
    True for `lakebench-spark-runner` when the SCC actually lists
    `lakebench-spark-runner-v2` -- exactly the LB-088 pattern moved rather
    than fixed. The fix parses the jsonpath output as individual tokens
    and checks exact equality.
    """

    def test_exact_match_wins(self):
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                return _make(cmd_returncode=1)
            if "scc" in cmd:
                return _make(
                    cmd_stdout=(
                        "system:serviceaccount:myns:lakebench-spark-runner\n"
                        "system:serviceaccount:otherns:some-other-sa\n"
                    )
                )
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is True

    def test_no_substring_false_positive_on_suffixed_sa(self):
        """The subagent-provided reproducer. Before the fix,
        `lakebench-spark-runner` in `[...lakebench-spark-runner-v2]`
        returned True and reported a grant that didn't exist."""
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                return _make(cmd_returncode=1)
            if "scc" in cmd:
                return _make(cmd_stdout="system:serviceaccount:myns:lakebench-spark-runner-v2\n")
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is False, (
            "an SCC granting only the -v2 SA must not report the base name as granted"
        )

    def test_empty_users_returns_false(self):
        verifier = SecurityVerifier(k8s=Mock())

        def fake_run(cmd, **kwargs):
            if "rolebinding" in cmd:
                return _make(cmd_returncode=1)
            if "scc" in cmd:
                return _make(cmd_stdout="")
            return _make(cmd_returncode=1)

        with patch("lakebench.k8s.security.subprocess.run", side_effect=fake_run):
            status = verifier._check_scc_assignment("anyuid", "lakebench-spark-runner", "myns")
        assert status.assigned is False
