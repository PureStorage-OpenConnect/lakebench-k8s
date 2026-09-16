"""Shared-cluster-resource destroy refcount tests.

Cluster-scoped resources (Stackable SecretClass, scratch StorageClass) must
not be deleted while another lakebench namespace still references them.
Doing so has crashed other users' Hive Metastore pods and killed other
deploys' PVC provisioning mid-run under parallel UAT.

The gate is a pure function so refcount behavior can be unit-tested
without a live cluster; the wiring itself needs a UAT round to fully
confirm (revert the fix, confirm 4-parallel destroy re-breaks Hive).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from lakebench.deploy.destroy import (
    _is_last_lakebench_namespace,
    _other_lakebench_namespaces_exist,
)


class TestIsLastLakebenchNamespace:
    def test_only_current_namespace(self):
        assert _is_last_lakebench_namespace(["v12-t01"], "v12-t01") is True

    def test_current_plus_others(self):
        assert _is_last_lakebench_namespace(["v12-t01", "v12-t02", "v12-t03"], "v12-t01") is False

    def test_current_not_in_list(self):
        # A defensive case: the current namespace is not visible in the list
        # (e.g. already deleted, or label mismatch). Others exist -> not last.
        assert _is_last_lakebench_namespace(["v12-t02", "v12-t03"], "v12-t01") is False

    def test_empty_list(self):
        # Nothing visible -- treat as last (safe: we won't delete a
        # shared resource we don't know is shared).
        assert _is_last_lakebench_namespace([], "v12-t01") is True


class TestOtherLakebenchNamespacesExist:
    def _make_core_v1(self, ns_names: list[str]) -> MagicMock:
        core_v1 = MagicMock()
        items = []
        for name in ns_names:
            item = MagicMock()
            item.metadata.name = name
            items.append(item)
        core_v1.list_namespace.return_value = MagicMock(items=items)
        return core_v1

    def test_only_current_returns_false(self):
        core = self._make_core_v1(["v12-t01"])
        assert _other_lakebench_namespaces_exist(core, "v12-t01") is False

    def test_others_exist_returns_true(self):
        core = self._make_core_v1(["v12-t01", "v12-t02"])
        assert _other_lakebench_namespaces_exist(core, "v12-t01") is True

    def test_list_failure_is_fail_safe(self):
        """A listing exception must return True (assume others exist) so we
        don't delete a shared cluster-scoped resource on a flaky read."""
        core = MagicMock()
        core.list_namespace.side_effect = RuntimeError("api down")
        assert _other_lakebench_namespaces_exist(core, "v12-t01") is True

    def test_uses_managed_by_lakebench_label(self):
        """The refcount must filter by the lakebench label -- otherwise a
        random unrelated namespace would count and shared resources would
        never be garbage-collected."""
        core = self._make_core_v1(["v12-t01"])
        _other_lakebench_namespaces_exist(core, "v12-t01")
        call = core.list_namespace.call_args
        assert call.kwargs.get("label_selector") == "app.kubernetes.io/managed-by=lakebench"
