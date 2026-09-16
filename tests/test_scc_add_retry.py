"""SCC add-scc-to-user must verify + retry.

`oc adm policy add-scc-to-user` on OpenShift 4.10+ creates a namespaced
RoleBinding `system:openshift:scc:<scc>` that binds the SA to a
ClusterRole -- NOT a mutation of the SCC's cluster-scoped `.users` array
(that legacy field stays empty on modern OCP). The original LB-088
finding assumed the legacy mechanism and a cross-namespace race that does
not apply on 4.10+.

The retry-with-verify remains useful within a single namespace (sequential
adds for postgres + spark-runner can race between phases) and to surface
real `oc` failures loud instead of silent skip. Verification now reads the
namespaced RoleBinding subjects rather than the always-empty SCC users.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.k8s.security import SecurityVerifier


def _mock_completed(returncode: int = 0, stdout: str = "", stderr: str = ""):
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


class TestSccAddRetry:
    def _make_verifier(self) -> SecurityVerifier:
        v = SecurityVerifier.__new__(SecurityVerifier)
        return v

    def test_add_succeeds_on_first_attempt(self):
        v = self._make_verifier()
        v._SCC_ADD_BACKOFF_SECONDS = 0
        with patch("lakebench.k8s.security.subprocess.run") as run:
            run.side_effect = [
                _mock_completed(0),  # add-scc-to-user
                # jsonpath output lists "<ns>/<name> <ns>/<name>" for each SA subject
                _mock_completed(0, stdout="v12-t01/lakebench-spark-runner "),
            ]
            assert v._add_scc("anyuid", "lakebench-spark-runner", "v12-t01") is True
        assert run.call_count == 2

    def test_add_retries_when_subject_did_not_land(self):
        """Race scenario: `add-scc-to-user` returned success but the SA
        wasn't in the RoleBinding subjects when we verified. Retry."""
        v = self._make_verifier()
        v._SCC_ADD_BACKOFF_SECONDS = 0
        with patch("lakebench.k8s.security.subprocess.run") as run:
            run.side_effect = [
                _mock_completed(0),  # add attempt 1
                _mock_completed(0, stdout="other/sa "),  # verify: not landed
                _mock_completed(0),  # add attempt 2
                _mock_completed(0, stdout="v12-t01/lakebench-spark-runner "),  # landed
            ]
            assert v._add_scc("anyuid", "lakebench-spark-runner", "v12-t01") is True

    def test_add_gives_up_after_max_attempts(self):
        v = self._make_verifier()
        v._SCC_ADD_BACKOFF_SECONDS = 0
        with patch("lakebench.k8s.security.subprocess.run") as run:
            run.side_effect = [_mock_completed(0), _mock_completed(0, stdout="")] * 10
            assert v._add_scc("anyuid", "lakebench-spark-runner", "v12-t01") is False
        # 4 adds + 4 verifies = 8 calls (default _SCC_ADD_MAX_ATTEMPTS = 4)
        assert run.call_count == 2 * v._SCC_ADD_MAX_ATTEMPTS

    def test_add_recovers_from_add_command_failure(self):
        """If the add command itself raises but a later attempt lands, succeed."""
        v = self._make_verifier()
        v._SCC_ADD_BACKOFF_SECONDS = 0
        with patch("lakebench.k8s.security.subprocess.run") as run:
            run.side_effect = [
                RuntimeError("transient"),  # add attempt 1 raises
                _mock_completed(0, stdout=""),  # verify: not landed
                _mock_completed(0),  # add attempt 2
                _mock_completed(0, stdout="v12-t01/lakebench-spark-runner "),
            ]
            assert v._add_scc("anyuid", "lakebench-spark-runner", "v12-t01") is True

    def test_verify_returns_false_on_bad_returncode(self):
        v = self._make_verifier()
        with patch("lakebench.k8s.security.subprocess.run") as run:
            run.return_value = _mock_completed(1, stderr="not found")
            assert v._scc_binding_has_subject("anyuid", "lakebench-spark-runner", "v12-t01") is False

    def test_verify_returns_false_on_exception(self):
        v = self._make_verifier()
        with patch("lakebench.k8s.security.subprocess.run", side_effect=RuntimeError("boom")):
            assert v._scc_binding_has_subject("anyuid", "lakebench-spark-runner", "v12-t01") is False

    def test_verify_matches_correct_namespace_only(self):
        """`v12-t01/spark` must not match when the binding only lists
        `v12-t02/spark`. jsonpath output is space-separated ns/name tokens."""
        v = self._make_verifier()
        with patch("lakebench.k8s.security.subprocess.run") as run:
            run.return_value = _mock_completed(
                0, stdout="v12-t02/lakebench-spark-runner other/sa "
            )
            assert (
                v._scc_binding_has_subject("anyuid", "lakebench-spark-runner", "v12-t01")
                is False
            )
